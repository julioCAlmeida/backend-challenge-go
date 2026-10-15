package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/julioCAlmeida/backend-challenge-go/internal/app"
)

// PendingConfig tunes the pending-reference worker.
type PendingConfig struct {
	PollInterval time.Duration
	BatchSize    int
}

// PendingWorker resumes PENDING_REFERENCE (and any committed PENDING)
// transactions whose next attempt is due. State lives in PostgreSQL, so the
// work survives restarts and is shared by every instance.
type PendingWorker struct {
	svc    *app.WageringService
	cfg    PendingConfig
	log    *slog.Logger
	runner *Runner
}

func NewPendingWorker(svc *app.WageringService, cfg PendingConfig, log *slog.Logger) *PendingWorker {
	return &PendingWorker{svc: svc, cfg: cfg, log: log.With(slog.String("worker", "pending-reference")), runner: NewRunner("pending-reference", log)}
}

func (w *PendingWorker) Start() { w.runner.Go(1, func(ctx context.Context, _ int) { w.loop(ctx) }) }

func (w *PendingWorker) Stop(ctx context.Context) error { return w.runner.Stop(ctx) }

func (w *PendingWorker) loop(ctx context.Context) {
	failures := 0
	for ctx.Err() == nil {
		n, err := w.svc.ResumePending(ctx, w.cfg.BatchSize)
		if err != nil && ctx.Err() == nil {
			failures++
			w.log.WarnContext(ctx, "pending scan failed", slog.Any("error", err), slog.Int("consecutiveFailures", failures))
			sleep(ctx, app.Backoff(w.cfg.PollInterval, 10*time.Second, failures))
			continue
		}
		failures = 0
		if n < w.cfg.BatchSize {
			sleep(ctx, w.cfg.PollInterval)
		}
	}
}
