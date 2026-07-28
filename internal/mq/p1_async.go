package mq

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nbhaohao/go-seckill/internal/deduct"
	"github.com/nbhaohao/go-seckill/internal/idgen"
	"github.com/nbhaohao/go-seckill/internal/order"
)

// rollbackTimeout bounds the compensating RollbackPreDeduct call with its own
// deadline; it must not reuse the caller ctx that just expired on ProduceSync.
const rollbackTimeout = 2 * time.Second

// NewProducer is m04 p1 S1. AI will implement it during p1.
//  1. SeedBrokers makes bootstrap addresses explicit; metadata still decides
//     the final leader, so this list is not a permanent broker routing table.
func NewProducer(brokers ...string) (*kgo.Client, error) {
	return kgo.NewClient(kgo.SeedBrokers(brokers...))
}

// NewConsumer is m04 p1 S2. AI will implement it during p1.
//  1. ConsumerGroup gives Kafka ownership and offsets; ConsumeTopics declares
//     the input. p2 will replace auto commit with an explicit commit boundary.
func NewConsumer(group, topic string, brokers ...string) (*kgo.Client, error) {
	return kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
}

// EnqueueOrder is m04 p1 S1. AI will implement it during p1.
//  1. Redis pre-deduction remains the hot-path stock gate from m03.
//  2. productID is the Kafka key so one product hashes to one partition and
//     preserves per-product order without serializing unrelated products.
//  3. ProduceSync observes broker acknowledgement before returning HTTP 202.
//  4. Produce failure must compensate the successful pre-deduction; this still
//     cannot close the process-crash window and is intentionally not an outbox.
func EnqueueOrder(ctx context.Context, rdb *redis.Client, producer *kgo.Client, topic string, req order.PlaceOrderRequest) (*AcceptedOrder, error) {
	if _, err := deduct.PreDeduct(ctx, rdb, req.ProductID, req.Quantity); err != nil {
		return nil, err
	}

	acceptedAt := time.Now()
	value, err := encodeOrderCreated(req, acceptedAt)
	if err != nil {
		rollbackPreDeduct(rdb, req)
		return nil, err
	}

	record := &kgo.Record{
		Topic: topic,
		Key:   []byte(strconv.FormatInt(req.ProductID, 10)),
		Value: value,
	}
	if err := producer.ProduceSync(ctx, record).FirstErr(); err != nil {
		rollbackPreDeduct(rdb, req)
		return nil, err
	}

	return &AcceptedOrder{RequestID: req.RequestID, Status: http.StatusAccepted, Accepted: acceptedAt}, nil
}

// rollbackPreDeduct compensates a successful PreDeduct on its own short
// context; the caller's ctx may have just expired on ProduceSync, and reusing
// it here would make go-redis reject the command before it reaches the wire.
func rollbackPreDeduct(rdb *redis.Client, req order.PlaceOrderRequest) {
	ctx, cancel := context.WithTimeout(context.Background(), rollbackTimeout)
	defer cancel()
	_ = deduct.RollbackPreDeduct(ctx, rdb, req.ProductID, req.Quantity)
}

// PollOne is m04 p1 S2. AI will implement it during p1.
//  1. PollFetches owns the blocking wait and reports fetch errors separately;
//     callers supply a deadline instead of guessing with time.Sleep.
func PollOne(ctx context.Context, consumer *kgo.Client) (*kgo.Record, error) {
	fetches := consumer.PollFetches(ctx)
	if errs := fetches.Errors(); len(errs) > 0 {
		return nil, errs[0].Err
	}
	iter := fetches.RecordIter()
	if iter.Done() {
		return nil, ErrNoRecord
	}
	return iter.Next(), nil
}

// PollBatch is m04 p1 S2. Kafka fetches are batches; returning every record is
// required because discarding the tail would silently skip in-memory work.
func PollBatch(ctx context.Context, consumer *kgo.Client) ([]*kgo.Record, error) {
	fetches := consumer.PollFetches(ctx)
	if errs := fetches.Errors(); len(errs) > 0 {
		return nil, errs[0].Err
	}
	var records []*kgo.Record
	iter := fetches.RecordIter()
	for !iter.Done() {
		records = append(records, iter.Next())
	}
	return records, nil
}

// PlaceRecord is m04 p1 S2. AI will implement it during p1.
//  1. The consumer calls m01 PlaceOrderTx rather than reimplementing stock or
//     idempotency; the unique request_id index remains the final duplicate judge.
func PlaceRecord(ctx context.Context, db *sqlx.DB, node *idgen.Node, record *kgo.Record) (*order.Order, error) {
	req, err := RequestFromRecord(record)
	if err != nil {
		return nil, err
	}
	return order.PlaceOrderTx(ctx, db, node, req)
}
