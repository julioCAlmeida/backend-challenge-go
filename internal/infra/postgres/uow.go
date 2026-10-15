package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/julioCAlmeida/backend-challenge-go/internal/app"
)

// UnitOfWork opens one pgx transaction per call and hands out repositories bound
// to it, which is how a single SQL transaction spans several repositories.
type UnitOfWork struct {
	pool *pgxpool.Pool
}

func NewUnitOfWork(pool *pgxpool.Pool) *UnitOfWork { return &UnitOfWork{pool: pool} }

var _ app.UnitOfWork = (*UnitOfWork)(nil)

// Within runs fn in a READ COMMITTED transaction. Correctness does not rely on
// the isolation level: wallet rows are locked explicitly (FOR UPDATE), balance
// updates are version-conditioned and the schema enforces the invariants.
func (u *UnitOfWork) Within(ctx context.Context, fn func(ctx context.Context, tx app.Tx) error) error {
	return u.run(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted}, fn)
}

// Snapshot runs fn in a READ ONLY REPEATABLE READ transaction.
func (u *UnitOfWork) Snapshot(ctx context.Context, fn func(ctx context.Context, tx app.Tx) error) error {
	return u.run(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}, fn)
}

func (u *UnitOfWork) run(ctx context.Context, opts pgx.TxOptions, fn func(ctx context.Context, tx app.Tx) error) (err error) {
	ptx, err := u.pool.BeginTx(ctx, opts)
	if err != nil {
		return Classify(err)
	}
	defer func() {
		if err != nil {
			// Rollback with a fresh context: the caller's may already be canceled.
			_ = ptx.Rollback(context.WithoutCancel(ctx))
		}
	}()
	if err = fn(ctx, &txRepos{q: ptx}); err != nil {
		return Classify(err)
	}
	if err = ptx.Commit(ctx); err != nil {
		// A failed COMMIT has an unknown outcome only if the connection dropped;
		// callers retry and idempotency resolves it.
		return Classify(err)
	}
	return nil
}

type txRepos struct{ q pgx.Tx }

func (t *txRepos) Wallets() app.WalletRepository           { return walletRepo{q: t.q} }
func (t *txRepos) Transactions() app.TransactionRepository { return transactionRepo{q: t.q} }
func (t *txRepos) Ledger() app.LedgerRepository            { return ledgerRepo{q: t.q} }
func (t *txRepos) Outbox() app.OutboxWriter                { return outboxWriter{q: t.q} }
func (t *txRepos) Inbox() app.InboxRepository              { return inboxRepo{q: t.q} }

func notFound(err error) bool { return errors.Is(err, pgx.ErrNoRows) }
