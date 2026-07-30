package overload

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestM05P1AdmissionFullFailsFastWithinBudget(t *testing.T) {
	admission := NewAdmission(2, 50*time.Millisecond)
	ctx := context.Background()
	if err := admission.Acquire(ctx); err != nil {
		t.Fatalf("acquire slot 1: %v", err)
	}
	if err := admission.Acquire(ctx); err != nil {
		t.Fatalf("acquire slot 2: %v", err)
	}

	start := time.Now()
	err := admission.Acquire(ctx)
	waited := time.Since(start)
	if !errors.Is(err, ErrAdmissionFull) {
		t.Fatalf("expected ErrAdmissionFull once both slots are held, got %v", err)
	}
	if waited > 500*time.Millisecond {
		t.Fatalf("full-admission rejection took too long to fail fast: %v", waited)
	}
	t.Logf("third acquire on a full 2-slot admission waited=%v before ErrAdmissionFull; stats=%+v", waited, admission.Stats())
}

func TestM05P1ReleaseReturnsSlotAndNoLeakOnCancel(t *testing.T) {
	admission := NewAdmission(1, 50*time.Millisecond)

	if err := admission.Acquire(context.Background()); err != nil {
		t.Fatalf("acquire the only slot: %v", err)
	}
	t.Logf("after first acquire: %+v", admission.Stats())

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := admission.Acquire(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected ctx.Err() on an already-cancelled acquire, got %v", err)
	}
	t.Logf("after cancelled acquire attempt against a full admission (must not occupy a slot): %+v", admission.Stats())

	admission.Release()
	t.Logf("after releasing the held slot: %+v", admission.Stats())

	if err := admission.Acquire(context.Background()); err != nil {
		t.Fatalf("slot should be free again after Release: %v", err)
	}
	admission.Release()

	stats := admission.Stats()
	if stats.InFlight != 0 {
		t.Fatalf("expected InFlight to drain back to zero, got %d (stats=%+v)", stats.InFlight, stats)
	}
	t.Logf("final stats after a full acquire/cancel/release/acquire/release cycle: %+v", stats)
}
