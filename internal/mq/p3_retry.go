package mq

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nbhaohao/go-seckill/internal/order"
)

// ProcessWithRetry is m04 p3. AI will implement it in two slices during p3.
//  1. Retry is bounded and exponential: permanent poison must not occupy one
//     partition forever, and each waited interval remains observable evidence.
//  2. After exhaustion, the original payload goes to DLT with cause, retry
//     count, and source coordinates; success means the caller may commit the
//     source offset so the following normal record can progress.
//  3. Retrying is safe only because every attempt keeps the same requestID and
//     PlaceOrderTx is idempotent through orders.uk_request_id.
func ProcessWithRetry(ctx context.Context, producer *kgo.Client, dltTopic string, record *kgo.Record, policy RetryPolicy, handler RecordHandler) (*order.Order, RetryResult, error) {
	panic("TODO: phase p3") // AI 将在 p3 S1/S2 按上面的 why 边界分切片实现。
}
