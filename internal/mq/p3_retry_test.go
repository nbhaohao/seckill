package mq

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nbhaohao/go-seckill/internal/order"
	"github.com/nbhaohao/go-seckill/internal/testutil"
)

func TestM04P3PoisonGoesToDLTWithReadableHeaders(t *testing.T) {
	admin := openKafka(t)
	dlt := uniqueName(t, "p3-dlt")
	createTopic(t, admin, dlt)
	original := &kgo.Record{Topic: "order.created", Partition: 2, Offset: 41, Key: []byte("10404"), Value: []byte("not-json")}
	policy := RetryPolicy{MaxAttempts: 3, BaseBackoff: 5 * time.Millisecond}
	_, result, err := ProcessWithRetry(context.Background(), admin, dlt, original, policy, func(context.Context, *kgo.Record) (*order.Order, error) {
		return nil, errors.New("poison payload")
	})
	if err != nil {
		t.Fatalf("process poison: %v", err)
	}
	if !result.DeadLettered || result.Attempts != 3 {
		t.Fatalf("unexpected retry result: %+v", result)
	}
	reader := openKafka(t, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{dlt: {0: kgo.NewOffset().AtStart(), 1: kgo.NewOffset().AtStart(), 2: kgo.NewOffset().AtStart()}}))
	dltRecord := pollUntilRecord(t, reader, 15*time.Second)
	headers := HeaderMap(dltRecord)
	for _, key := range []string{"failure-reason", "retry-count", "original-topic", "original-partition", "original-offset"} {
		if headers[key] == "" {
			t.Fatalf("missing DLT header %q in %#v", key, headers)
		}
	}
	t.Logf("retry backoffs=%v; DLT headers=%v", result.Backoffs, headers)
}

func TestM04P3NormalAfterPoisonStillProcesses(t *testing.T) {
	env := openM04Env(t)
	const productID int64 = 10405
	testutil.ResetProduct(t, env.db, productID, 1)
	topic, dlt, group := uniqueName(t, "p3-orders"), uniqueName(t, "p3-dlt"), uniqueName(t, "p3-group")
	admin := openKafka(t)
	createTopic(t, admin, topic)
	createTopic(t, admin, dlt)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	poison := &kgo.Record{Topic: topic, Key: []byte("10405"), Value: []byte("not-json")}
	goodValue, _ := encodeOrderCreated(reqFor(productID, "m04-p3-good"), time.Now())
	good := &kgo.Record{Topic: topic, Key: []byte("10405"), Value: goodValue}
	if err := admin.ProduceSync(ctx, poison, good).FirstErr(); err != nil {
		t.Fatalf("produce poison+good: %v", err)
	}

	consumer, err := NewManualCommitConsumer(group, topic, testBroker)
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	t.Cleanup(consumer.Close)
	pollCtx, pollCancel := context.WithTimeout(context.Background(), 15*time.Second)
	records, err := PollBatch(pollCtx, consumer)
	pollCancel()
	if err != nil || len(records) < 2 {
		t.Fatalf("poll poison+normal batch: records=%d err=%v", len(records), err)
	}
	first := records[0]
	_, outcome, err := ProcessWithRetry(context.Background(), admin, dlt, first, RetryPolicy{MaxAttempts: 2, BaseBackoff: 5 * time.Millisecond}, func(ctx context.Context, record *kgo.Record) (*order.Order, error) {
		return PlaceRecord(ctx, env.db, env.node, record)
	})
	if err != nil || !outcome.DeadLettered {
		t.Fatalf("poison outcome=%+v err=%v", outcome, err)
	}
	if err := CommitProcessed(context.Background(), consumer, first); err != nil {
		t.Fatalf("commit poison after DLT: %v", err)
	}
	second := records[1]
	if _, err := PlaceRecord(context.Background(), env.db, env.node, second); err != nil {
		t.Fatalf("normal record after poison: %v", err)
	}
	if err := CommitProcessed(context.Background(), consumer, second); err != nil {
		t.Fatalf("commit normal: %v", err)
	}
	if count := countOrders(t, env.db, productID); count != 1 {
		t.Fatalf("normal message behind poison must be processed: orders=%d", count)
	}
	t.Logf("poison moved to DLT after %d attempts; following normal request %s still created one order", outcome.Attempts, "m04-p3-good")
}

func TestM04P3RetriesReuseIdempotencyKey(t *testing.T) {
	env := openM04Env(t)
	const productID int64 = 10406
	testutil.ResetProduct(t, env.db, productID, 2)
	value, _ := encodeOrderCreated(reqFor(productID, "m04-p3-retry-same-key"), time.Now())
	record := &kgo.Record{Topic: "order.created", Value: value}
	producer := openKafka(t)
	attempt := 0
	o, result, err := ProcessWithRetry(context.Background(), producer, uniqueName(t, "unused-dlt"), record, RetryPolicy{MaxAttempts: 3, BaseBackoff: 5 * time.Millisecond}, func(ctx context.Context, record *kgo.Record) (*order.Order, error) {
		attempt++
		placed, err := PlaceRecord(ctx, env.db, env.node, record)
		if err != nil {
			return nil, err
		}
		if attempt < 3 {
			return nil, errors.New("ack path failed after DB commit")
		}
		return placed, nil
	})
	if err != nil {
		t.Fatalf("retry idempotency: %v", err)
	}
	if result.Attempts != 3 || o == nil || countOrders(t, env.db, productID) != 1 {
		t.Fatalf("retries must keep one DB row: result=%+v order=%v rows=%d", result, o, countOrders(t, env.db, productID))
	}
	t.Logf("request_id m04-p3-retry-same-key attempted %d times; DB rows remained 1 and order id=%d", result.Attempts, o.ID)
}
