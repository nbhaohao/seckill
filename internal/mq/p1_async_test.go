package mq

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nbhaohao/go-seckill/internal/deduct"
	"github.com/nbhaohao/go-seckill/internal/testutil"
)

func TestM04P1AsyncOrderEventuallyPersistsAndIdentityHolds(t *testing.T) {
	env := openM04Env(t)
	const productID int64 = 10401
	const total = 8
	testutil.ResetProduct(t, env.db, productID, total)
	testutil.DeleteKeys(t, env.rdb, deduct.StockKey(productID))
	if err := deduct.WarmStock(context.Background(), env.rdb, env.db, productID); err != nil {
		t.Fatalf("warm stock: %v", err)
	}

	topic, group := uniqueName(t, "p1-orders"), uniqueName(t, "p1-group")
	admin := openKafka(t)
	createTopic(t, admin, topic)
	producer, err := NewProducer(testBroker)
	if err != nil {
		t.Fatalf("new producer: %v", err)
	}
	t.Cleanup(producer.Close)
	consumer, err := NewConsumer(group, topic, testBroker)
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	t.Cleanup(consumer.Close)

	var firstAccepted time.Time
	for i := 0; i < total; i++ {
		accepted, err := EnqueueOrder(context.Background(), env.rdb, producer, topic, reqFor(productID, uniqueName(t, "p1-request")))
		if err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
		if accepted.Status != http.StatusAccepted {
			t.Fatalf("expected HTTP semantic 202, got %d", accepted.Status)
		}
		if i == 0 {
			firstAccepted = accepted.Accepted
		}
	}

	deadline := time.Now().Add(20 * time.Second)
	for countOrders(t, env.db, productID) < total && time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Until(deadline))
		records, err := PollBatch(ctx, consumer)
		cancel()
		if err != nil {
			t.Fatalf("poll batch: %v", err)
		}
		for _, record := range records {
			if _, err := PlaceRecord(context.Background(), env.db, env.node, record); err != nil {
				t.Fatalf("place record: %v", err)
			}
		}
	}
	visibleAt := time.Now()
	orders, stock := countOrders(t, env.db, productID), productStock(t, env.db, productID)
	if orders != total || total-stock != orders {
		t.Fatalf("eventual identity broken: initial=%d stock=%d orders=%d", total, stock, orders)
	}
	t.Logf("eventual consistency window: first HTTP 202 at %s -> DB reached %d orders at %s (%s)", firstAccepted.Format(time.RFC3339Nano), total, visibleAt.Format(time.RFC3339Nano), visibleAt.Sub(firstAccepted))
}

func TestM04P1ProduceFailureRollsBackPreDeduct(t *testing.T) {
	env := openM04Env(t)
	const productID int64 = 10402
	testutil.ResetProduct(t, env.db, productID, 3)
	testutil.DeleteKeys(t, env.rdb, deduct.StockKey(productID))
	if err := deduct.WarmStock(context.Background(), env.rdb, env.db, productID); err != nil {
		t.Fatalf("warm stock: %v", err)
	}

	bad, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:1"), kgo.DialTimeout(200*time.Millisecond), kgo.RequestTimeoutOverhead(200*time.Millisecond))
	if err != nil {
		t.Fatalf("bad producer: %v", err)
	}
	t.Cleanup(bad.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 1200*time.Millisecond)
	defer cancel()
	if _, err := EnqueueOrder(ctx, env.rdb, bad, "unreachable", reqFor(productID, uniqueName(t, "p1-fail"))); err == nil {
		t.Fatal("expected produce failure")
	}
	remaining, err := env.rdb.Get(context.Background(), deduct.StockKey(productID)).Int()
	if err != nil {
		t.Fatalf("read redis stock: %v", err)
	}
	if remaining != 3 {
		t.Fatalf("produce failure must roll back pre-deduction: got %d want 3", remaining)
	}
	t.Logf("produce failed after pre-deduction; Redis stock restored to %d", remaining)
}
