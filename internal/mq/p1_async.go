package mq

import (
	"context"

	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nbhaohao/go-seckill/internal/idgen"
	"github.com/nbhaohao/go-seckill/internal/order"
)

// NewProducer is m04 p1 S1. AI will implement it during p1.
//  1. SeedBrokers makes bootstrap addresses explicit; metadata still decides
//     the final leader, so this list is not a permanent broker routing table.
func NewProducer(brokers ...string) (*kgo.Client, error) {
	panic("TODO: phase p1") // AI 将在 p1 S1 按上面的 why 边界实现。
}

// NewConsumer is m04 p1 S2. AI will implement it during p1.
//  1. ConsumerGroup gives Kafka ownership and offsets; ConsumeTopics declares
//     the input. p2 will replace auto commit with an explicit commit boundary.
func NewConsumer(group, topic string, brokers ...string) (*kgo.Client, error) {
	panic("TODO: phase p1") // AI 将在 p1 S2 按上面的 why 边界实现。
}

// EnqueueOrder is m04 p1 S1. AI will implement it during p1.
//  1. Redis pre-deduction remains the hot-path stock gate from m03.
//  2. productID is the Kafka key so one product hashes to one partition and
//     preserves per-product order without serializing unrelated products.
//  3. ProduceSync observes broker acknowledgement before returning HTTP 202.
//  4. Produce failure must compensate the successful pre-deduction; this still
//     cannot close the process-crash window and is intentionally not an outbox.
func EnqueueOrder(ctx context.Context, rdb *redis.Client, producer *kgo.Client, topic string, req order.PlaceOrderRequest) (*AcceptedOrder, error) {
	panic("TODO: phase p1") // AI 将在 p1 S1 按上面的 why 边界实现。
}

// PollOne is m04 p1 S2. AI will implement it during p1.
//  1. PollFetches owns the blocking wait and reports fetch errors separately;
//     callers supply a deadline instead of guessing with time.Sleep.
func PollOne(ctx context.Context, consumer *kgo.Client) (*kgo.Record, error) {
	panic("TODO: phase p1") // AI 将在 p1 S2 按上面的 why 边界实现。
}

// PollBatch is m04 p1 S2. Kafka fetches are batches; returning every record is
// required because discarding the tail would silently skip in-memory work.
func PollBatch(ctx context.Context, consumer *kgo.Client) ([]*kgo.Record, error) {
	panic("TODO: phase p1") // AI 将在 p1 S2 按上面的 why 边界实现。
}

// PlaceRecord is m04 p1 S2. AI will implement it during p1.
//  1. The consumer calls m01 PlaceOrderTx rather than reimplementing stock or
//     idempotency; the unique request_id index remains the final duplicate judge.
func PlaceRecord(ctx context.Context, db *sqlx.DB, node *idgen.Node, record *kgo.Record) (*order.Order, error) {
	panic("TODO: phase p1") // AI 将在 p1 S2 按上面的 why 边界实现。
}
