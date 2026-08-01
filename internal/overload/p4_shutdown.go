package overload

import (
	"context"
	"time"
)

// Shutdown is m05 p4. AI will implement it in two slices during p4.
//  1. Steps run in the declared order, sharing the caller's one ctx/overall
//     deadline. Graceful shutdown has a real ordering dependency (stop
//     accepting → drain in-flight → stop consumers → flush producers → close
//     dependencies), and one shared deadline is what keeps that ordering
//     honest instead of every step inventing its own private timeout.
//  2. Checking the deadline before starting the next step — rather than
//     trusting every step to respect ctx internally — turns a stuck step into
//     an observable, reported stopping point (Failed + ErrShutdownTimeout)
//     instead of a silent wedge with no evidence of where it hung.
func Shutdown(ctx context.Context, steps []ShutdownStep) ShutdownReport {
	start := time.Now()
	report := ShutdownReport{}

	for _, step := range steps {
		select {
		case <-ctx.Done():
			report.Failed = step.Name
			report.Err = ErrShutdownTimeout
			report.Elapsed = time.Since(start)
			return report
		default:
		}

		if err := step.Fn(ctx); err != nil {
			report.Failed = step.Name
			report.Err = err
			report.Elapsed = time.Since(start)
			return report
		}
		report.Completed = append(report.Completed, step.Name)
	}

	report.Elapsed = time.Since(start)
	return report
}
