package overload

import (
	"context"
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
	panic("TODO: phase p4") // AI 将在 p4 S1+S2 按上面的 why 边界实现。
}
