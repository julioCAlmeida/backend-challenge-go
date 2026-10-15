package worker

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/julioCAlmeida/backend-challenge-go/internal/app"
	"github.com/julioCAlmeida/backend-challenge-go/internal/faults"
	"github.com/julioCAlmeida/backend-challenge-go/internal/infra/postgres"
	"github.com/julioCAlmeida/backend-challenge-go/internal/infra/sqsx"
	"github.com/julioCAlmeida/backend-challenge-go/internal/observability"
)

// sqsBatchLimit is the maximum number of entries of SendMessageBatch.
const sqsBatchLimit = 10

// EventPublisher sends events to the broker.
type EventPublisher interface {
	PublishBatch(ctx context.Context, entries []sqsx.BatchEntry) (map[string]error, error)
}

// OutboxConfig tunes the outbox publisher.
type OutboxConfig struct {
	Owner        string
	Workers      int
	PollInterval time.Duration
	BatchSize    int
	Lease        time.Duration
	BackoffBase  time.Duration
	BackoffMax   time.Duration
}

// OutboxPublisher publishes committed outbox events. Every instance runs it
// with several goroutines; all of them, across instances, compete through
// leases (FOR UPDATE SKIP LOCKED + locked_until), and an event whose publisher
// died is reclaimed after its lease expires. Publication is at-least-once and
// always reuses the stored eventId (SQS MessageDeduplicationId and the
// eventId attribute), so republications are idempotent for consumers.
type OutboxPublisher struct {
	store     *postgres.OutboxStore
	publisher EventPublisher
	cfg       OutboxConfig
	log       *slog.Logger
	metrics   *observability.Metrics
	faults    *faults.Injector
	runner    *Runner
}

func NewOutboxPublisher(store *postgres.OutboxStore, pub EventPublisher, cfg OutboxConfig, log *slog.Logger, m *observability.Metrics, f *faults.Injector) *OutboxPublisher {
	if cfg.Owner == "" {
		cfg.Owner = uuid.NewString()
	}
	if cfg.Workers < 1 {
		cfg.Workers = 1
	}
	return &OutboxPublisher{store: store, publisher: pub, cfg: cfg, log: log.With(slog.String("worker", "outbox-publisher")),
		metrics: m, faults: f, runner: NewRunner("outbox-publisher", log)}
}

func (p *OutboxPublisher) Start() { p.runner.Go(p.cfg.Workers, p.loop) }

// Stop finishes the batch being published and releases the remaining leases so
// another instance can publish them without waiting for the lease to expire.
func (p *OutboxPublisher) Stop(ctx context.Context) error {
	err := p.runner.Stop(ctx)
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	defer cancel()
	if rerr := p.store.Release(rctx, p.cfg.Owner); rerr != nil {
		p.log.Warn("could not release outbox leases", slog.Any("error", rerr))
	}
	return err
}

func (p *OutboxPublisher) loop(ctx context.Context, idx int) {
	failures := 0
	for ctx.Err() == nil {
		n, err := p.PublishBatch(ctx)
		if err != nil {
			failures++
			p.log.WarnContext(ctx, "outbox batch failed", slog.Any("error", err))
			sleep(ctx, app.Backoff(p.cfg.PollInterval, 10*time.Second, failures))
			continue
		}
		failures = 0
		if idx == 0 {
			p.observeLag(ctx)
		}
		if n < p.cfg.BatchSize {
			sleep(ctx, p.cfg.PollInterval)
		}
	}
}

// PublishBatch claims and publishes one batch; it returns how many were claimed.
func (p *OutboxPublisher) PublishBatch(ctx context.Context) (int, error) {
	recs, err := p.store.Claim(ctx, p.cfg.Owner, p.cfg.Lease, p.cfg.BatchSize)
	if err != nil {
		return 0, err
	}
	if len(recs) > 0 {
		p.faults.Crash(faults.OutboxAfterClaim)
	}
	for i := 0; i < len(recs); i += sqsBatchLimit {
		if ctx.Err() != nil {
			// Remaining leases are released by Stop.
			return i, nil
		}
		p.publishChunk(ctx, recs[i:min(i+sqsBatchLimit, len(recs))])
	}
	return len(recs), nil
}

func (p *OutboxPublisher) publishChunk(loopCtx context.Context, recs []postgres.OutboxRecord) {
	// In-flight publication is not interrupted by shutdown.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(loopCtx), 10*time.Second)
	defer cancel()
	entries := make([]sqsx.BatchEntry, len(recs))
	for i, rec := range recs {
		entries[i] = sqsx.BatchEntry{
			ID: strconv.Itoa(i), GroupID: rec.PartitionKey, DedupID: rec.EventID.String(), Body: rec.Payload,
			Attrs: map[string]string{"eventId": rec.EventID.String(), "eventType": rec.EventType, "aggregateId": rec.AggregateID},
		}
	}
	failed, err := p.publisher.PublishBatch(ctx, entries)
	if err != nil {
		failed = map[string]error{}
		for i := range recs {
			failed[strconv.Itoa(i)] = err
		}
	}
	published := make([]uuid.UUID, 0, len(recs))
	for i, rec := range recs {
		ferr, isFailed := failed[strconv.Itoa(i)]
		if !isFailed {
			published = append(published, rec.EventID)
			continue
		}
		p.metrics.OutboxPublishFailed.Inc()
		p.metrics.Retry("outbox")
		retryIn := app.Backoff(p.cfg.BackoffBase, p.cfg.BackoffMax, rec.Attempts-1)
		p.log.WarnContext(ctx, "outbox publish failed", slog.String("eventId", rec.EventID.String()),
			slog.Int("attempts", rec.Attempts), slog.Duration("retryIn", retryIn), slog.Any("error", ferr))
		if merr := p.store.MarkFailed(ctx, rec.EventID, p.cfg.Owner, retryIn, ferr.Error()); merr != nil {
			p.log.WarnContext(ctx, "could not record outbox failure; lease expiry will retry", slog.Any("error", merr))
		}
	}
	if len(published) == 0 {
		return
	}
	p.faults.Crash(faults.OutboxAfterPublish)
	if err := p.store.MarkPublishedMany(ctx, published); err != nil {
		// Published but not confirmed: they will be republished with the same eventId.
		p.log.WarnContext(ctx, "could not confirm publication", slog.Int("events", len(published)), slog.Any("error", err))
		return
	}
	p.metrics.OutboxPublished.Add(float64(len(published)))
	for i, rec := range recs {
		if _, isFailed := failed[strconv.Itoa(i)]; !isFailed {
			p.metrics.OutboxPublishLatency.Observe(time.Since(rec.OccurredAt).Seconds())
		}
	}
}

func (p *OutboxPublisher) observeLag(ctx context.Context) {
	lag, n, err := p.store.Lag(ctx)
	if err != nil {
		return
	}
	p.metrics.OutboxLagSeconds.Set(lag.Seconds())
	p.metrics.OutboxPending.Set(float64(n))
}
