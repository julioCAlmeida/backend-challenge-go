//go:build integration

package integration

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/julioCAlmeida/backend-challenge-go/internal/app"
	"github.com/julioCAlmeida/backend-challenge-go/internal/domain/events"
	"github.com/julioCAlmeida/backend-challenge-go/internal/domain/money"
	"github.com/julioCAlmeida/backend-challenge-go/internal/domain/wallet"
	"github.com/julioCAlmeida/backend-challenge-go/internal/infra/postgres"
)

func TestMigrationsApplyAndRevert(t *testing.T) {
	db := newDB(t)
	tables := func() int64 {
		return db.scalar(t, `SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public'
			AND table_name IN ('wallets','wager_transactions','wallet_ledger_entries','inbox_messages','outbox_events')`)
	}
	if n := tables(); n != 5 {
		t.Fatalf("tables after up: %d", n)
	}
	m, err := postgres.NewMigrator(db.OwnerURL)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Down(); err != nil {
		t.Fatalf("down: %v", err)
	}
	if n := tables(); n != 0 {
		t.Fatalf("tables after down: %d", n)
	}
	if err := m.Up(); err != nil {
		t.Fatalf("up again: %v", err)
	}
	v, dirty, err := m.Version()
	if err != nil || dirty || v != 2 {
		t.Fatalf("version %d dirty %v err %v", v, dirty, err)
	}
	m.Close()
	if n := tables(); n != 5 {
		t.Fatalf("tables after re-up: %d", n)
	}
}

func pgCode(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}

// seed creates a wallet with an opening credit through the real use case.
func seed(t *testing.T, db *testDB, balance string) (*pgxpool.Pool, wallet.Snapshot) {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), db.AppURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	svc := app.NewWalletService(postgres.NewUnitOfWork(pool), app.SystemClock{}, app.UUIDv7{}, app.NopMetrics{}, discardLogger())
	m, _ := money.Parse(balance, money.BRL)
	w, err := svc.Open(context.Background(), app.OpenWalletCommand{PlayerID: uuid.New(), InitialBalance: m, Meta: events.Meta{CorrelationID: "seed"}})
	if err != nil {
		t.Fatal(err)
	}
	return pool, w
}

func TestLedgerIsAppendOnly(t *testing.T) {
	db := newDB(t)
	appPool, w := seed(t, db, "100.00")
	ctx := context.Background()

	// Even the schema owner is blocked by the protection triggers.
	for _, sql := range []string{
		`UPDATE wallet_ledger_entries SET amount_minor = 1`,
		`DELETE FROM wallet_ledger_entries`,
		`TRUNCATE wallet_ledger_entries`,
	} {
		if _, err := db.Owner.Exec(ctx, sql); err == nil || !strings.Contains(err.Error(), "append-only") {
			t.Errorf("owner %q: %v", sql, err)
		}
	}
	// The application role does not even have the privileges.
	for _, sql := range []string{`UPDATE wallet_ledger_entries SET amount_minor = 1`, `DELETE FROM wallet_ledger_entries`} {
		if _, err := appPool.Exec(ctx, sql); pgCode(err) != "42501" {
			t.Errorf("app role %q: %v", sql, err)
		}
	}
	if _, err := appPool.Exec(ctx, `DELETE FROM wallets WHERE id = $1`, w.ID); pgCode(err) != "42501" {
		t.Errorf("app role delete wallet: %v", err)
	}
	if n := db.scalar(t, `SELECT COUNT(*) FROM wallet_ledger_entries`); n != 1 {
		t.Fatalf("ledger rows %d", n)
	}
}

func TestSchemaEnforcesFinancialInvariants(t *testing.T) {
	db := newDB(t)
	_, w := seed(t, db, "100.00")
	ctx := context.Background()
	exec := func(sql string, args ...any) error {
		_, err := db.Owner.Exec(ctx, sql, args...)
		return err
	}
	opening := db.scalar(t, `SELECT 1 FROM wager_transactions WHERE wallet_id = $1 AND kind = 'OPENING'`, w.ID)
	if opening != 1 {
		t.Fatal("opening missing")
	}

	// Non-negative balance.
	if err := exec(`UPDATE wallets SET balance_minor = -1, version = version + 1 WHERE id = $1`, w.ID); pgCode(err) != "23514" {
		t.Errorf("negative balance: %v", err)
	}
	// Balance change without the matching ledger entry fails at COMMIT (deferred trigger).
	if err := exec(`UPDATE wallets SET balance_minor = 5000, version = version + 1 WHERE id = $1`, w.ID); err == nil || !strings.Contains(err.Error(), "no matching ledger entry") {
		t.Errorf("balance without ledger: %v", err)
	}
	// Version must move with the balance.
	if err := exec(`UPDATE wallets SET version = version + 1 WHERE id = $1`, w.ID); err == nil {
		t.Error("version bump without balance change accepted")
	}
	// Duplicate wallet for (player, currency).
	if err := exec(`INSERT INTO wallets VALUES ($1, $2, 'BRL', 0, 1, now(), now())`, uuid.New(), w.PlayerID); pgCode(err) != "23505" {
		t.Errorf("duplicate wallet: %v", err)
	}
	// Second OPENING for the same wallet.
	if err := exec(`INSERT INTO wager_transactions (id, origin, kind, status, wallet_id, player_id, amount_minor, currency,
		result_balance_minor, created_at, updated_at, completed_at)
		VALUES ($1, 'INTERNAL', 'OPENING', 'PROCESSED', $2, $3, 100, 'BRL', 100, now(), now(), now())`, uuid.New(), w.ID, w.PlayerID); pgCode(err) != "23505" {
		t.Errorf("duplicate opening: %v", err)
	}
	// An OPENING cannot carry external metadata; an external op must not be OPENING.
	if err := exec(`INSERT INTO wager_transactions (id, origin, kind, status, provider_id, external_transaction_id, idempotency_key,
		payload_hash, wallet_id, player_id, round_id, game_id, amount_minor, currency, created_at, updated_at)
		VALUES ($1, 'EXTERNAL', 'OPENING', 'PENDING', 'p', 'e', 'k', repeat('a', 64), $2, $3, 'r', 'g', 100, 'BRL', now(), now())`,
		uuid.New(), w.ID, w.PlayerID); pgCode(err) != "23514" {
		t.Errorf("external OPENING: %v", err)
	}
	// LOSS must be zero, BET positive.
	insertExt := func(kind string, amount int64, ext string) error {
		return exec(`INSERT INTO wager_transactions (id, origin, kind, status, provider_id, external_transaction_id, idempotency_key,
			payload_hash, wallet_id, player_id, round_id, game_id, amount_minor, currency, created_at, updated_at)
			VALUES ($1, 'EXTERNAL', $2, 'PENDING', 'p', $3, $3, repeat('a', 64), $4, $5, 'r', 'g', $6, 'BRL', now(), now())`,
			uuid.New(), kind, ext, w.ID, w.PlayerID, amount)
	}
	if err := insertExt("LOSS", 1, "x1"); pgCode(err) != "23514" {
		t.Errorf("LOSS with amount: %v", err)
	}
	if err := insertExt("BET", 0, "x2"); pgCode(err) != "23514" {
		t.Errorf("BET zero: %v", err)
	}
	// Unique (provider, idempotency key) and (provider, external id).
	if err := insertExt("BET", 1, "dup"); err != nil {
		t.Fatal(err)
	}
	if err := insertExt("BET", 1, "dup"); pgCode(err) != "23505" {
		t.Errorf("duplicate key: %v", err)
	}
	// Ledger arithmetic and uniqueness.
	txID := db.scalarUUID(t, `SELECT id FROM wager_transactions WHERE wallet_id = $1 AND kind = 'OPENING'`, w.ID)
	dupID := db.scalarUUID(t, `SELECT id FROM wager_transactions WHERE external_transaction_id = 'dup'`)
	if err := exec(`INSERT INTO wallet_ledger_entries (id, wallet_id, transaction_id, direction, amount_minor, currency,
		balance_before_minor, balance_after_minor, wallet_version, created_at) VALUES ($1, $2, $3, 'CREDIT', 10, 'BRL', 0, 11, 9, now())`,
		uuid.New(), w.ID, dupID); pgCode(err) != "23514" {
		t.Errorf("bad arithmetic: %v", err)
	}
	if err := exec(`INSERT INTO wallet_ledger_entries (id, wallet_id, transaction_id, direction, amount_minor, currency,
		balance_before_minor, balance_after_minor, wallet_version, created_at) VALUES ($1, $2, $3, 'CREDIT', 10, 'BRL', 0, 10, 9, now())`,
		uuid.New(), w.ID, txID); pgCode(err) != "23505" {
		t.Errorf("duplicate (wallet, transaction): %v", err)
	}
	// Terminal transactions are immutable.
	if err := exec(`UPDATE wager_transactions SET status = 'REJECTED', failure_code = 'X' WHERE id = $1`, txID); err == nil || !strings.Contains(err.Error(), "terminal") {
		t.Errorf("terminal update: %v", err)
	}
	if err := exec(`DELETE FROM wager_transactions WHERE id = $1`, txID); err == nil {
		t.Error("transaction delete accepted")
	}
}

func (db *testDB) scalarUUID(t *testing.T, sql string, args ...any) uuid.UUID {
	t.Helper()
	var v uuid.UUID
	if err := db.Owner.QueryRow(context.Background(), sql, args...).Scan(&v); err != nil {
		t.Fatal(err)
	}
	return v
}

func TestSingleReversalIsEnforcedByTheDatabase(t *testing.T) {
	db := newDB(t)
	_, w := seed(t, db, "100.00")
	ctx := context.Background()
	bet := uuid.New()
	ins := func(id uuid.UUID, kind, ext string, ref *uuid.UUID, status string) error {
		var refExt *string
		completed := "now()"
		failure := "NULL"
		result := "100"
		if ref != nil {
			s := "bet-1"
			refExt = &s
		}
		if status == "PENDING" {
			completed, result = "NULL", "NULL"
		}
		_, err := db.Owner.Exec(ctx, `INSERT INTO wager_transactions (id, origin, kind, status, provider_id, external_transaction_id,
			idempotency_key, payload_hash, wallet_id, player_id, round_id, game_id, amount_minor, currency,
			reference_external_transaction_id, reference_transaction_id, result_balance_minor, failure_code, created_at, updated_at, completed_at)
			VALUES ($1, 'EXTERNAL', $2, $3, 'p', $4, $4, repeat('a', 64), $5, $6, 'r', 'g', 100, 'BRL', $7, $8, `+result+`, `+failure+`, now(), now(), `+completed+`)`,
			id, kind, status, ext, w.ID, w.PlayerID, refExt, ref)
		return err
	}
	if err := ins(bet, "BET", "bet-1", nil, "PROCESSED"); err != nil {
		t.Fatal(err)
	}
	if err := ins(uuid.New(), "REFUND", "refund-1", &bet, "PROCESSED"); err != nil {
		t.Fatal(err)
	}
	if err := ins(uuid.New(), "ROLLBACK", "rollback-1", &bet, "PROCESSED"); pgCode(err) != "23505" {
		t.Fatalf("second reversal: %v", err)
	}
}

// TestFinancialChangesAreAtomic: a failure anywhere in the unit of work leaves
// no partial state (balance, ledger, transaction, outbox).
func TestFinancialChangesAreAtomic(t *testing.T) {
	db := newDB(t)
	pool, w := seed(t, db, "100.00")
	uow := postgres.NewUnitOfWork(pool)
	ctx := context.Background()
	before := db.scalar(t, `SELECT (SELECT COUNT(*) FROM wallet_ledger_entries) + (SELECT COUNT(*) FROM outbox_events) + (SELECT COUNT(*) FROM wager_transactions)`)

	boom := errors.New("boom")
	err := uow.Within(ctx, func(ctx context.Context, tx app.Tx) error {
		wl, err := tx.Wallets().GetForUpdate(ctx, w.ID)
		if err != nil {
			return err
		}
		m, _ := money.Parse("10.00", money.BRL)
		entry, err := wl.Credit(m, uuid.New(), uuid.New(), time.Now())
		if err != nil {
			return err
		}
		if err := tx.Wallets().UpdateBalance(ctx, wl); err != nil {
			return err
		}
		_ = entry // the ledger entry is never written...
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
	// ...and a commit without the ledger entry is refused by the database.
	err = uow.Within(ctx, func(ctx context.Context, tx app.Tx) error {
		wl, err := tx.Wallets().GetForUpdate(ctx, w.ID)
		if err != nil {
			return err
		}
		m, _ := money.Parse("10.00", money.BRL)
		if _, err := wl.Credit(m, uuid.New(), uuid.New(), time.Now()); err != nil {
			return err
		}
		return tx.Wallets().UpdateBalance(ctx, wl)
	})
	if err == nil || !strings.Contains(err.Error(), "ledger") {
		t.Fatalf("commit without ledger: %v", err)
	}
	after := db.scalar(t, `SELECT (SELECT COUNT(*) FROM wallet_ledger_entries) + (SELECT COUNT(*) FROM outbox_events) + (SELECT COUNT(*) FROM wager_transactions)`)
	if before != after || db.scalar(t, `SELECT balance_minor FROM wallets WHERE id = $1`, w.ID) != 10000 {
		t.Fatal("partial state persisted")
	}
}
