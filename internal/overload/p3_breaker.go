package overload

import (
	"context"
	"sync"
	"time"
)

// writeRetryAfterSeconds is the RetryAfter hint attached to write-path 503/429
// responses; a fixed small value keeps p3 about correctness (never faking a
// success), not about tuning a backoff curve — that belongs to p5's workload.
const writeRetryAfterSeconds = 1

// Breaker is m05 p3. AI will implement it in three slices during p3.
//  1. Tripping on consecutive failures (not a failure percentage) keeps the
//     state machine simple and needs no rolling window; opening on trip stops
//     calling a dependency that is already down instead of piling more
//     failing calls behind the ones already failing.
//  2. Cooling down for OpenFor before allowing only HalfOpenProbes concurrent
//     probes balances two risks: hammering a maybe-still-broken dependency
//     with a full flood, versus waiting forever for one lonely probe to
//     happen to land and prove recovery.
//  3. The write path can never report a success it did not actually achieve —
//     masking backpressure as 2xx would let a caller believe an order was
//     placed when it was not, corrupting the correctness the rest of the
//     system depends on. The read path may trade freshness for availability
//     because a stale read is still true, just old; that split is why
//     Degraded is only ever legal on the read path.
type Breaker struct {
	mu     sync.Mutex
	policy BreakerPolicy
	now    func() time.Time

	state          BreakerState
	failures       int
	openedAt       time.Time
	probesInFlight int
}

// NewBreaker is m05 p3 S1. AI will implement it during p3.
func NewBreaker(policy BreakerPolicy) *Breaker {
	now := policy.Now
	if now == nil {
		now = time.Now
	}
	return &Breaker{policy: policy, now: now}
}

// Do is m05 p3 S1+S2. AI will implement it during p3.
func (b *Breaker) Do(ctx context.Context, fn func(context.Context) error) error {
	b.mu.Lock()
	now := b.now()
	b.checkTransitionLocked(now)

	switch b.state {
	case StateOpen:
		b.mu.Unlock()
		return ErrBreakerOpen
	case StateHalfOpen:
		if b.probesInFlight >= b.policy.HalfOpenProbes {
			b.mu.Unlock()
			return ErrBreakerOpen
		}
		b.probesInFlight++
	}
	probing := b.state == StateHalfOpen
	b.mu.Unlock()

	err := fn(ctx)

	b.mu.Lock()
	defer b.mu.Unlock()

	if probing {
		b.probesInFlight--
		if err == nil {
			b.state = StateClosed
			b.failures = 0
		} else {
			b.state = StateOpen
			b.openedAt = b.now()
		}
		return err
	}

	if err == nil {
		b.failures = 0
		return nil
	}
	b.failures++
	if b.failures >= b.policy.FailureThreshold {
		b.state = StateOpen
		b.openedAt = b.now()
	}
	return err
}

// checkTransitionLocked promotes an expired open state to half-open. Callers
// must hold b.mu.
func (b *Breaker) checkTransitionLocked(now time.Time) {
	if b.state == StateOpen && now.Sub(b.openedAt) >= b.policy.OpenFor {
		b.state = StateHalfOpen
		b.probesInFlight = 0
	}
}

// State is m05 p3 S1 (observation). AI will implement it during p3.
func (b *Breaker) State() BreakerState {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.checkTransitionLocked(b.now())
	return b.state
}

// WritePathFailure is m05 p3 S3. AI will implement it during p3.
func WritePathFailure(err error) (int, DegradedPayload) {
	panic("TODO: phase p3") // AI 将在 p3 S3 按上面的 why 边界实现。
}

// ReadPathFallback is m05 p3 S3. AI will implement it during p3.
func ReadPathFallback(err error, staleAvailable bool) (int, DegradedPayload) {
	panic("TODO: phase p3") // AI 将在 p3 S3 按上面的 why 边界实现。
}
