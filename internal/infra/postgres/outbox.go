package postgres

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/julioCAlmeida/backend-challenge-go/internal/domain/events"
)

type outboxWriter struct{ q querier }

// Append stores the immutable event snapshots in the caller's transaction.
func (w outboxWriter) Append(ctx context.Context, evs ...events.Event) error {
	for _, e := range evs {
		payload, err := e.Payload()
		if err != nil {
			return err
		}
		if _, err := w.q.Exec(ctx, `INSERT INTO outbox_events
			(event_id, aggregate_type, aggregate_id, partition_key, event_type, payload, occurred_at, next_attempt_at)
			VALUES ($1, $2, $3, $4, $5, $6::json, $7, $7)`,
			e.ID(), e.AggregateType(), e.AggregateID(), e.PartitionKey(), e.Type(), string(payload), e.OccurredAt()); err != nil {
			return err
		}
	}
	return nil
}

// OutboxRecord is a claimed event ready to publish.
type OutboxRecord struct {
	EventID      uuid.UUID
	EventType    string
	AggregateID  string
	PartitionKey string
	Payload      string
	Attempts     int
	OccurredAt   time.Time
}

// OutboxStore is used by the publisher worker, outside business transactions.
// Leases use the database clock so instances with skewed clocks agree.
type OutboxStore struct {
	pool *pgxpool.Pool
}

func NewOutboxStore(pool *pgxpool.Pool) *OutboxStore { return &OutboxStore{pool: pool} }

// Claim leases up to limit due events to owner. SKIP LOCKED lets several
// publishers claim disjoint batches; an expired lease (crashed publisher) makes
// the event claimable again.
func (s *OutboxStore) Claim(ctx context.Context, owner string, lease time.Duration, limit int) ([]OutboxRecord, error) {
	rows, err := s.pool.Query(ctx, `WITH due AS (
			SELECT event_id FROM outbox_events
			WHERE published_at IS NULL AND next_attempt_at <= now()
			  AND (locked_until IS NULL OR locked_until < now())
			ORDER BY occurred_at, event_id
			LIMIT $3
			FOR UPDATE SKIP LOCKED)
		UPDATE outbox_events o
		SET locked_by = $1, locked_until = now() + ($2::bigint * interval '1 millisecond'), attempts = o.attempts + 1
		FROM due WHERE o.event_id = due.event_id
		RETURNING o.event_id, o.event_type, o.aggregate_id, o.partition_key, o.payload::text, o.attempts, o.occurred_at`,
		owner, lease.Milliseconds(), limit)
	if err != nil {
		return nil, Classify(err)
	}
	recs, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (OutboxRecord, error) {
		var r OutboxRecord
		err := row.Scan(&r.EventID, &r.EventType, &r.AggregateID, &r.PartitionKey, &r.Payload, &r.Attempts, &r.OccurredAt)
		return r, err
	})
	return recs, Classify(err)
}

// MarkPublishedMany confirms several events in one statement.
func (s *OutboxStore) MarkPublishedMany(ctx context.Context, ids []uuid.UUID) error {
	if len(ids) == 0 {
		return nil
	}
	_, err := s.pool.Exec(ctx, `UPDATE outbox_events SET published_at = now(), locked_by = NULL, locked_until = NULL, last_error = NULL
		WHERE event_id = ANY($1) AND published_at IS NULL`, ids)
	return Classify(err)
}

// MarkFailed releases the lease and schedules a retry.
func (s *OutboxStore) MarkFailed(ctx context.Context, id uuid.UUID, owner string, retryIn time.Duration, cause string) error {
	if len(cause) > 500 {
		cause = cause[:500]
	}
	_, err := s.pool.Exec(ctx, `UPDATE outbox_events
		SET locked_by = NULL, locked_until = NULL, last_error = $3,
		    next_attempt_at = now() + ($4::bigint * interval '1 millisecond')
		WHERE event_id = $1 AND locked_by = $2 AND published_at IS NULL`, id, owner, cause, retryIn.Milliseconds())
	return Classify(err)
}

// Release gives back leases on shutdown so another instance takes over immediately.
func (s *OutboxStore) Release(ctx context.Context, owner string) error {
	_, err := s.pool.Exec(ctx, `UPDATE outbox_events SET locked_by = NULL, locked_until = NULL
		WHERE locked_by = $1 AND published_at IS NULL`, owner)
	return Classify(err)
}

// Lag returns the age of the oldest unpublished event (0 when none) and the
// number of unpublished events.
func (s *OutboxStore) Lag(ctx context.Context) (time.Duration, int64, error) {
	var ms, n int64
	err := s.pool.QueryRow(ctx, `SELECT
			COALESCE((EXTRACT(EPOCH FROM (now() - MIN(occurred_at))) * 1000)::bigint, 0), COUNT(*)
		FROM outbox_events WHERE published_at IS NULL`).Scan(&ms, &n)
	if err != nil {
		return 0, 0, Classify(err)
	}
	return time.Duration(ms) * time.Millisecond, n, nil
}
