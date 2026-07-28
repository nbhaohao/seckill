package mq

import (
	"context"
	"testing"
	"time"

	"github.com/nbhaohao/go-seckill/internal/testutil"
)

func TestM04P2UncommittedRecordReplaysButCreatesOneOrder(t *testing.T) {
	env := openM04Env(t)
	const productID int64 = 10403
	testutil.ResetProduct(t, env.db, productID, 2)
	topic, group := uniqueName(t, "p2-orders"), uniqueName(t, "p2-group")
	admin := openKafka(t)
	createTopic(t, admin, topic)
	produceRaw(t, admin, topic, reqFor(productID, "m04-p2-same-request"))

	firstConsumer, err := NewManualCommitConsumer(group, topic, testBroker)
	if err != nil {
		t.Fatalf("new first consumer: %v", err)
	}
	first := pollUntilRecord(t, firstConsumer, 15*time.Second)
	firstOrder, err := PlaceRecord(context.Background(), env.db, env.node, first)
	if err != nil {
		t.Fatalf("first place: %v", err)
	}
	// Deliberately omit CommitProcessed: this is the crash window under test.
	firstConsumer.Close()

	secondConsumer, err := NewManualCommitConsumer(group, topic, testBroker)
	if err != nil {
		t.Fatalf("new restarted consumer: %v", err)
	}
	t.Cleanup(secondConsumer.Close)
	replayed := pollUntilRecord(t, secondConsumer, 15*time.Second)
	secondOrder, err := PlaceRecord(context.Background(), env.db, env.node, replayed)
	if err != nil {
		t.Fatalf("replayed place: %v", err)
	}
	commitCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := CommitProcessed(commitCtx, secondConsumer, replayed); err != nil {
		t.Fatalf("commit replayed record: %v", err)
	}
	if firstOrder.ID != secondOrder.ID || countOrders(t, env.db, productID) != 1 {
		t.Fatalf("replay must return existing order: first=%d second=%d rows=%d", firstOrder.ID, secondOrder.ID, countOrders(t, env.db, productID))
	}
	t.Logf("same Kafka message consumed 2 times; order id stayed %d and DB rows stayed 1", firstOrder.ID)
}
