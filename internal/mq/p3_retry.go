package mq

import (
	"context"
	"time"

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
	var result RetryResult
	var lastErr error
	for attempt := 1; attempt <= policy.MaxAttempts; attempt++ {
		result.Attempts = attempt
		placed, err := handler(ctx, record)
		if err == nil {
			return placed, result, nil
		}
		lastErr = err
		if attempt == policy.MaxAttempts {
			break
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

	dltRecord := &kgo.Record{
		Topic:   dltTopic,
		Key:     record.Key,
		Value:   record.Value,
		Headers: dltHeaders(record, lastErr, result.Attempts),
	}
	if produceErr := producer.ProduceSync(ctx, dltRecord).FirstErr(); produceErr != nil {
		return nil, result, produceErr
	}

	result.DeadLettered = true
	return nil, result, nil
}
