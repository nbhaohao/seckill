package overload

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestM05P3OpensAfterConsecutiveFailuresAndShortCircuits(t *testing.T) {
	now, _ := FakeClock(time.Unix(0, 0))
	breaker := NewBreaker(BreakerPolicy{FailureThreshold: 3, OpenFor: time.Second, HalfOpenProbes: 1, Now: now})

	var calls int
	failing := func(context.Context) error {
		calls++
		return errors.New("downstream boom")
	}

	var states []string
	for i := 0; i < 3; i++ {
		if err := breaker.Do(context.Background(), failing); err == nil {
			t.Fatalf("attempt %d: expected the failure to propagate while closed, got nil", i)
		}
		states = append(states, breaker.State().String())
	}
	if got := breaker.State(); got != StateOpen {
		t.Fatalf("expected open after 3 consecutive failures with threshold=3, got %s", got)
	}
	callsAtOpen := calls

	for i := 0; i < 3; i++ {
		if err := breaker.Do(context.Background(), failing); !errors.Is(err, ErrBreakerOpen) {
			t.Fatalf("attempt %d: expected ErrBreakerOpen once open, got %v", i, err)
		}
	}
	if calls != callsAtOpen {
		t.Fatalf("an open breaker must short-circuit without calling fn: calls at open=%d calls now=%d", callsAtOpen, calls)
	}
	t.Logf("state timeline while tripping=%v; fn call count stayed at %d across 3 more Do() calls once open", states, calls)
}

func TestM05P3HalfOpenLimitsProbesThenCloses(t *testing.T) {
	now, advance := FakeClock(time.Unix(0, 0))
	policy := BreakerPolicy{FailureThreshold: 1, OpenFor: 100 * time.Millisecond, HalfOpenProbes: 2, Now: now}
	breaker := NewBreaker(policy)

	if err := breaker.Do(context.Background(), func(context.Context) error {
		return errors.New("boom")
	}); err == nil {
		t.Fatalf("expected the first failure to propagate")
	}
	if got := breaker.State(); got != StateOpen {
		t.Fatalf("expected open after a single failure with threshold=1, got %s", got)
	}

	advance(200 * time.Millisecond)
	if got := breaker.State(); got != StateHalfOpen {
		t.Fatalf("expected half-open once OpenFor has elapsed, got %s", got)
	}

	const attempts = 5
	release := make(chan struct{})
	var probesStarted int64
	var rejected int64
	var wg sync.WaitGroup
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := breaker.Do(context.Background(), func(context.Context) error {
				atomic.AddInt64(&probesStarted, 1)
				<-release
				return nil
			})
			switch {
			case err == nil:
			case errors.Is(err, ErrBreakerOpen):
				atomic.AddInt64(&rejected, 1)
			default:
				t.Errorf("unexpected probe error: %v", err)
			}
		}()
	}
	deadline := time.Now().Add(2 * time.Second)
	for atomic.LoadInt64(&probesStarted) < int64(policy.HalfOpenProbes) && time.Now().Before(deadline) {
		<-time.After(time.Millisecond)
	}
	close(release)
	wg.Wait()

	if probesStarted != int64(policy.HalfOpenProbes) {
		t.Fatalf("expected exactly HalfOpenProbes=%d admitted probes, got %d", policy.HalfOpenProbes, probesStarted)
	}
	if rejected != attempts-int64(policy.HalfOpenProbes) {
		t.Fatalf("expected the remaining %d calls rejected with ErrBreakerOpen, got %d", attempts-int64(policy.HalfOpenProbes), rejected)
	}
	if got := breaker.State(); got != StateClosed {
		t.Fatalf("expected closed after a successful half-open probe, got %s", got)
	}
	t.Logf("half-open round: admitted probes=%d (== HalfOpenProbes) rejected=%d state now=%s", probesStarted, rejected, breaker.State())

	// Second round: trip it again, then let the lone half-open probe fail.
	if err := breaker.Do(context.Background(), func(context.Context) error {
		return errors.New("boom again")
	}); err == nil {
		t.Fatalf("expected the second-round failure to propagate")
	}
	if got := breaker.State(); got != StateOpen {
		t.Fatalf("expected open again after a threshold=1 failure, got %s", got)
	}
	advance(200 * time.Millisecond)
	if got := breaker.State(); got != StateHalfOpen {
		t.Fatalf("expected half-open again after the second cooldown, got %s", got)
	}
	if err := breaker.Do(context.Background(), func(context.Context) error {
		return errors.New("probe fails")
	}); err == nil {
		t.Fatalf("expected the failing half-open probe's error to propagate")
	}
	if got := breaker.State(); got != StateOpen {
		t.Fatalf("expected a failing half-open probe to reopen the breaker, got %s", got)
	}
	t.Logf("full state timeline: closed -> open -> half-open -> closed -> open -> half-open -> open")
}

func TestM05P3WritePathNeverReportsSuccess(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{"breaker_open", ErrBreakerOpen},
		{"rate_limited", ErrRateLimited},
		{"admission_full", ErrAdmissionFull},
		{"context_deadline_exceeded", context.DeadlineExceeded},
		{"custom_downstream_error", errors.New("downstream exploded")},
	}
	for _, tc := range cases {
		status, payload := WritePathFailure(tc.err)
		if status >= 200 && status < 300 {
			t.Fatalf("%s: write path must never report a 2xx success, got status=%d", tc.name, status)
		}
		if payload.Degraded {
			t.Fatalf("%s: write path must never set Degraded=true, got %+v", tc.name, payload)
		}
		t.Logf("write-path failure %s -> status=%d payload=%+v", tc.name, status, payload)
	}
}
