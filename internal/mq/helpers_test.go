package mq

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nbhaohao/go-seckill/internal/idgen"
	"github.com/nbhaohao/go-seckill/internal/order"
	"github.com/nbhaohao/go-seckill/internal/testutil"
)

const testBroker = "127.0.0.1:9092"

type m04Env struct {
	db   *sqlx.DB
	rdb  *redis.Client
	node *idgen.Node
}

func openM04Env(t *testing.T) m04Env {
	t.Helper()
	db := testutil.OpenTestDB(t)
	t.Cleanup(func() { _ = db.Close() })
	rdb := testutil.OpenTestRedis(t)
	node, err := idgen.NewNode(17)
	if err != nil {
		t.Fatalf("idgen.NewNode: %v", err)
	}
	return m04Env{db: db, rdb: rdb, node: node}
}

func openKafka(t *testing.T, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	base := []kgo.Opt{kgo.SeedBrokers(envOrTest("TEST_KAFKA_BROKERS", testBroker))}
	client, err := kgo.NewClient(append(base, opts...)...)
	if err != nil {
		t.Fatalf("kgo.NewClient: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx); err != nil {
		client.Close()
		t.Skipf("skip integration test: cannot connect Kafka (%v); run `docker compose up -d kafka` first", err)
	}
	t.Cleanup(client.Close)
	return client
}

func envOrTest(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func uniqueName(t *testing.T, suffix string) string {
	t.Helper()
	return fmt.Sprintf("m04-%s-%d", suffix, time.Now().UnixNano())
}

func createTopic(t *testing.T, client *kgo.Client, topic string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := kadm.NewClient(client).CreateTopic(ctx, 3, 1, nil, topic); err != nil {
		t.Fatalf("create topic %s: %v", topic, err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = kadm.NewClient(client).DeleteTopic(cleanupCtx, topic)
	})
}

func reqFor(productID int64, requestID string) order.PlaceOrderRequest {
	return order.PlaceOrderRequest{RequestID: requestID, ProductID: productID, UserID: 42, Quantity: 1}
}

func produceRaw(t *testing.T, client *kgo.Client, topic string, req order.PlaceOrderRequest) {
	t.Helper()
	value, err := encodeOrderCreated(req, time.Now())
	if err != nil {
		t.Fatalf("encode order.created: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: []byte(strconv.FormatInt(req.ProductID, 10)), Value: value}).FirstErr(); err != nil {
		t.Fatalf("produce %s: %v", topic, err)
	}
}

func pollUntilRecord(t *testing.T, client *kgo.Client, timeout time.Duration) *kgo.Record {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	record, err := PollOne(ctx, client)
	if err != nil {
		t.Fatalf("poll record: %v", err)
	}
	return record
}

func countOrders(t *testing.T, db *sqlx.DB, productID int64) int {
	t.Helper()
	var count int
	if err := db.Get(&count, "SELECT COUNT(*) FROM orders WHERE product_id = ?", productID); err != nil {
		t.Fatalf("count orders: %v", err)
	}
	return count
}

func productStock(t *testing.T, db *sqlx.DB, productID int64) int {
	t.Helper()
	var stock int
	if err := db.Get(&stock, "SELECT stock FROM products WHERE id = ?", productID); err != nil {
		t.Fatalf("read stock: %v", err)
	}
	return stock
}
