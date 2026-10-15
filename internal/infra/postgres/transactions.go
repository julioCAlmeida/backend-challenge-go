package postgres

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/julioCAlmeida/backend-challenge-go/internal/app"
	"github.com/julioCAlmeida/backend-challenge-go/internal/domain/money"
	"github.com/julioCAlmeida/backend-challenge-go/internal/domain/wager"
)

type transactionRepo struct{ q querier }

const txColumns = `id, origin, kind, status, provider_id, external_transaction_id, idempotency_key, payload_hash,
	wallet_id, player_id, round_id, game_id, amount_minor, currency, reference_external_transaction_id,
	reference_transaction_id, failure_code, result_balance_minor, reference_attempts, next_attempt_at,
	correlation_id, causation_id, created_at, updated_at, completed_at`

const txInsert = `INSERT INTO wager_transactions (` + txColumns + `)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25)`

func txArgs(s wager.Snapshot) []any {
	var resultBalance *int64
	if s.ResultBalance != nil {
		v := s.ResultBalance.MinorUnits()
		resultBalance = &v
	}
	return []any{
		s.ID, string(s.Origin), string(s.Kind), string(s.Status), nullable(s.ProviderID), nullable(s.ExternalTransactionID),
		nullable(s.IdempotencyKey), nullable(s.PayloadHash), s.WalletID, s.PlayerID, nullable(s.RoundID), nullable(s.GameID),
		s.Money.MinorUnits(), s.Money.Currency().Code(), nullable(s.ReferenceExternalTransactionID), s.ReferenceTransactionID,
		nullable(string(s.FailureCode)), resultBalance, s.ReferenceAttempts, s.NextAttemptAt,
		nullable(s.CorrelationID), nullable(s.CausationID), s.CreatedAt, s.UpdatedAt, s.CompletedAt,
	}
}

func nullable(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func (r transactionRepo) Insert(ctx context.Context, t *wager.Transaction) error {
	_, err := r.q.Exec(ctx, txInsert, txArgs(t.Snapshot())...)
	return err
}

// InsertIfAbsent claims the idempotency slot. If a concurrent transaction holds
// the same key/external id, PostgreSQL waits for it to finish before deciding,
// so the caller then sees the committed row.
func (r transactionRepo) InsertIfAbsent(ctx context.Context, t *wager.Transaction) (bool, error) {
	tag, err := r.q.Exec(ctx, txInsert+` ON CONFLICT DO NOTHING`, txArgs(t.Snapshot())...)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

func (r transactionRepo) Update(ctx context.Context, t *wager.Transaction) error {
	s := t.Snapshot()
	var resultBalance *int64
	if s.ResultBalance != nil {
		v := s.ResultBalance.MinorUnits()
		resultBalance = &v
	}
	tag, err := r.q.Exec(ctx, `UPDATE wager_transactions SET status = $2, reference_transaction_id = $3,
		failure_code = $4, result_balance_minor = $5, reference_attempts = $6, next_attempt_at = $7,
		updated_at = $8, completed_at = $9 WHERE id = $1`,
		s.ID, string(s.Status), s.ReferenceTransactionID, nullable(string(s.FailureCode)), resultBalance,
		s.ReferenceAttempts, s.NextAttemptAt, s.UpdatedAt, s.CompletedAt)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return wager.ErrTransactionNotFound
	}
	return nil
}

func (r transactionRepo) Get(ctx context.Context, id uuid.UUID) (*wager.Transaction, error) {
	t, err := r.one(ctx, `SELECT `+txColumns+` FROM wager_transactions WHERE id = $1`, id)
	if err == nil && t == nil {
		return nil, wager.ErrTransactionNotFound
	}
	return t, err
}

func (r transactionRepo) GetForUpdate(ctx context.Context, id uuid.UUID) (*wager.Transaction, error) {
	t, err := r.one(ctx, `SELECT `+txColumns+` FROM wager_transactions WHERE id = $1 FOR UPDATE`, id)
	if err == nil && t == nil {
		return nil, wager.ErrTransactionNotFound
	}
	return t, err
}

func (r transactionRepo) FindByKeyOrExternalID(ctx context.Context, providerID, key, externalID string) ([]*wager.Transaction, error) {
	rows, err := r.q.Query(ctx, `SELECT `+txColumns+` FROM wager_transactions
		WHERE provider_id = $1 AND (idempotency_key = $2 OR external_transaction_id = $3)`, providerID, key, externalID)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (*wager.Transaction, error) { return scanTransaction(row) })
}

func (r transactionRepo) FindByExternalID(ctx context.Context, providerID, externalID string) (*wager.Transaction, error) {
	return r.one(ctx, `SELECT `+txColumns+` FROM wager_transactions WHERE provider_id = $1 AND external_transaction_id = $2`, providerID, externalID)
}

func (r transactionRepo) HasProcessedReversal(ctx context.Context, referenceID uuid.UUID) (bool, error) {
	var exists bool
	err := r.q.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM wager_transactions
		WHERE reference_transaction_id = $1 AND status = 'PROCESSED' AND kind IN ('REFUND', 'ROLLBACK'))`, referenceID).Scan(&exists)
	return exists, err
}

func (r transactionRepo) WakeWaiting(ctx context.Context, providerID, externalID string, at time.Time) error {
	_, err := r.q.Exec(ctx, `UPDATE wager_transactions SET next_attempt_at = $3
		WHERE provider_id = $1 AND reference_external_transaction_id = $2 AND status = 'PENDING_REFERENCE' AND next_attempt_at > $3`,
		providerID, externalID, at)
	return err
}

func (r transactionRepo) DuePending(ctx context.Context, now time.Time, limit int) ([]app.PendingRef, error) {
	rows, err := r.q.Query(ctx, `SELECT id, wallet_id FROM wager_transactions
		WHERE status = 'PENDING' OR (status = 'PENDING_REFERENCE' AND next_attempt_at <= $1)
		ORDER BY next_attempt_at NULLS FIRST LIMIT $2`, now, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (app.PendingRef, error) {
		var p app.PendingRef
		err := row.Scan(&p.TransactionID, &p.WalletID)
		return p, err
	})
}

func (r transactionRepo) one(ctx context.Context, sql string, args ...any) (*wager.Transaction, error) {
	t, err := scanTransaction(r.q.QueryRow(ctx, sql, args...))
	if notFound(err) {
		return nil, nil
	}
	return t, err
}

func scanTransaction(row pgx.Row) (*wager.Transaction, error) {
	var (
		s                                            wager.Snapshot
		origin, kind, status, currency               string
		provider, external, key, hash, round, game   *string
		refExternal, failure, correlation, causation *string
		amount                                       int64
		resultBalance                                *int64
	)
	err := row.Scan(&s.ID, &origin, &kind, &status, &provider, &external, &key, &hash,
		&s.WalletID, &s.PlayerID, &round, &game, &amount, &currency, &refExternal, &s.ReferenceTransactionID,
		&failure, &resultBalance, &s.ReferenceAttempts, &s.NextAttemptAt, &correlation, &causation,
		&s.CreatedAt, &s.UpdatedAt, &s.CompletedAt)
	if err != nil {
		return nil, err
	}
	m, err := toMoney(amount, currency)
	if err != nil {
		return nil, err
	}
	s.Origin, s.Kind, s.Status, s.Money = wager.Origin(origin), wager.Kind(kind), wager.Status(status), m
	s.ProviderID, s.ExternalTransactionID, s.IdempotencyKey = deref(provider), deref(external), deref(key)
	s.PayloadHash, s.RoundID, s.GameID = deref(hash), deref(round), deref(game)
	s.ReferenceExternalTransactionID, s.FailureCode = deref(refExternal), wager.FailureCode(deref(failure))
	s.CorrelationID, s.CausationID = deref(correlation), deref(causation)
	if resultBalance != nil {
		rb, err := money.FromMinor(*resultBalance, m.Currency())
		if err != nil {
			return nil, err
		}
		s.ResultBalance = &rb
	}
	return wager.Rehydrate(s)
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
