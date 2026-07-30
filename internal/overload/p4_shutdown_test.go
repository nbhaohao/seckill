package overload

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestM05P4RunsStepsInDeclaredOrder(t *testing.T) {
	var mu sync.Mutex
	var executed []string
	record := func(name string) ShutdownStep {
		return ShutdownStep{Name: name, Fn: func(context.Context) error {
			mu.Lock()
			executed = append(executed, name)
			mu.Unlock()
			return nil
		}}
	}
	declared := []string{"stop-http", "drain-inflight", "stop-consumer", "flush-producer", "close-deps"}
	steps := make([]ShutdownStep, 0, len(declared))
	for _, name := range declared {
		steps = append(steps, record(name))
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	report := Shutdown(ctx, steps)

	if !reflect.DeepEqual(executed, declared) {
		t.Fatalf("actual execution order=%v want declared order=%v", executed, declared)
	}
	if !reflect.DeepEqual(report.Completed, declared) {
		t.Fatalf("report.Completed=%v want=%v", report.Completed, declared)
	}
	if report.Failed != "" || report.Err != nil {
		t.Fatalf("expected a clean shutdown, got Failed=%q Err=%v", report.Failed, report.Err)
	}
	t.Logf("actual execution order=%v; report.Completed=%v Elapsed=%v", executed, report.Completed, report.Elapsed)
}

func TestM05P4TimeoutStopsRemainingStepsButKeepsProgress(t *testing.T) {
	var mu sync.Mutex
	var started []string
	record := func(name string) {
		mu.Lock()
		started = append(started, name)
		mu.Unlock()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	steps := []ShutdownStep{
		{Name: "stop-http", Fn: func(context.Context) error {
			record("stop-http")
			return nil
		}},
		{Name: "drain-inflight", Fn: func(ctx context.Context) error {
			record("drain-inflight")
			<-ctx.Done() // simulate a step slow enough to spend the whole shutdown deadline, but it still finishes
			return nil
		}},
		{Name: "stop-consumer", Fn: func(context.Context) error {
			record("stop-consumer")
			return nil
		}},
	}

	report := Shutdown(ctx, steps)

	mu.Lock()
	gotStarted := append([]string(nil), started...)
	mu.Unlock()

	wantStarted := []string{"stop-http", "drain-inflight"}
	if !reflect.DeepEqual(gotStarted, wantStarted) {
		t.Fatalf("stop-consumer must never start once the shutdown deadline is spent: started=%v want=%v", gotStarted, wantStarted)
	}
	if !reflect.DeepEqual(report.Completed, wantStarted) {
		t.Fatalf("expected stop-http and drain-inflight completed before the deadline, got %v", report.Completed)
	}
	if report.Failed != "stop-consumer" {
		t.Fatalf("expected stop-consumer recorded as the step that never got to start, got %q", report.Failed)
	}
	if !errors.Is(report.Err, ErrShutdownTimeout) {
		t.Fatalf("expected ErrShutdownTimeout, got %v", report.Err)
	}
	t.Logf("Completed=%v Failed=%q Err=%v Elapsed=%v", report.Completed, report.Failed, report.Err, report.Elapsed)
}
