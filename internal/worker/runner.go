// Package worker contains the background workers: SQS consumer, outbox
// publisher and pending-reference retrier. Each one is started and stopped by
// the Fx lifecycle and finishes observably (Stop returns when it is done).
package worker

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// Runner runs goroutines under a cancelable context and waits for them on Stop.
type Runner struct {
	name    string
	log     *slog.Logger
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	started bool
}

func NewRunner(name string, log *slog.Logger) *Runner {
	return &Runner{name: name, log: log.With(slog.String("worker", name))}
}

// Go starts n goroutines running fn until the context is canceled.
func (r *Runner) Go(n int, fn func(ctx context.Context, idx int)) {
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	r.started = true
	for i := 0; i < n; i++ {
		r.wg.Add(1)
		go func(i int) {
			defer r.wg.Done()
			fn(ctx, i)
		}(i)
	}
	r.log.Info("worker started", slog.Int("goroutines", n))
}

// Stop cancels the loops and waits for them until ctx expires.
func (r *Runner) Stop(ctx context.Context) error {
	if !r.started {
		return nil
	}
	start := time.Now()
	r.cancel()
	done := make(chan struct{})
	go func() { r.wg.Wait(); close(done) }()
	select {
	case <-done:
		r.log.Info("worker stopped", slog.Duration("took", time.Since(start)))
		return nil
	case <-ctx.Done():
		return fmt.Errorf("worker %s did not stop before the deadline: %w", r.name, ctx.Err())
	}
}

// sleep waits d or until ctx is canceled; it returns false on cancellation.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
