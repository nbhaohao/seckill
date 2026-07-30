package mq

import (
	"context"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nbhaohao/go-seckill/internal/order"
)

// retryWithBackoff is m04 p3 S1. It bounds attempts to policy.MaxAttempts and
// waits an exponentially growing, cancellable interval between them; the last
// failed attempt never waits since there is no next attempt to reach.
func retryWithBackoff(ctx context.Context, record *kgo.Record, policy RetryPolicy, handler RecordHandler) (*order.Order, RetryResult, error) {
	var result RetryResult
	for attempt := 1; attempt <= policy.MaxAttempts; attempt++ {
		result.Attempts = attempt
		placed, err := handler(ctx, record)
		if err == nil {
			return placed, result, nil
		}
		if attempt == policy.MaxAttempts {
			return nil, result, err
		}
		backoff := policy.BaseBackoff << (attempt - 1)
		result.Backoffs = append(result.Backoffs, backoff)
		timer := time.NewTimer(backoff)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return nil, result, ctx.Err()
		}
	}
	panic("unreachable: loop above always returns")
}

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
