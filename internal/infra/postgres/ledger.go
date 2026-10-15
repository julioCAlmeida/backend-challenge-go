package postgres

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/julioCAlmeida/backend-challenge-go/internal/app"
	"github.com/julioCAlmeida/backend-challenge-go/internal/domain/wallet"
)

type ledgerRepo struct{ q querier }

func (r ledgerRepo) Append(ctx context.Context, e wallet.LedgerEntry) error {
	_, err := r.q.Exec(ctx, `INSERT INTO wallet_ledger_entries
		(id, wallet_id, transaction_id, direction, amount_minor, currency, balance_before_minor, balance_after_minor, wallet_version, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		e.ID(), e.WalletID(), e.TransactionID(), string(e.Direction()), e.Amount().MinorUnits(), e.Amount().Currency().Code(),
		e.BalanceBefore().MinorUnits(), e.BalanceAfter().MinorUnits(), e.WalletVersion(), e.CreatedAt())
	return err
}

func (r ledgerRepo) List(ctx context.Context, walletID uuid.UUID, afterSeq int64, limit int) ([]app.LedgerRow, error) {
	rows, err := r.q.Query(ctx, `SELECT seq, id, wallet_id, transaction_id, direction, amount_minor, currency,
		balance_before_minor, balance_after_minor, wallet_version, created_at
		FROM wallet_ledger_entries WHERE wallet_id = $1 AND seq > $2 ORDER BY seq LIMIT $3`, walletID, afterSeq, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (app.LedgerRow, error) {
		var (
			out                   app.LedgerRow
			s                     wallet.LedgerEntrySnapshot
			direction, currency   string
			amount, before, after int64
		)
		if err := row.Scan(&out.Seq, &s.ID, &s.WalletID, &s.TransactionID, &direction, &amount, &currency,
			&before, &after, &s.WalletVersion, &s.CreatedAt); err != nil {
			return out, err
		}
		var err error
		if s.Amount, err = toMoney(amount, currency); err != nil {
			return out, err
		}
		if s.BalanceBefore, err = toMoney(before, currency); err != nil {
			return out, err
		}
		if s.BalanceAfter, err = toMoney(after, currency); err != nil {
			return out, err
		}
		s.Direction = wallet.Direction(direction)
		out.Entry, err = wallet.NewLedgerEntry(s)
		return out, err
	})
}

// Totals sums credits and debits in SQL with NUMERIC accumulation (no overflow
// during the sum); the results are converted back to int64 minor units.
func (r ledgerRepo) Totals(ctx context.Context, walletID uuid.UUID) (app.LedgerTotals, error) {
	var t app.LedgerTotals
	err := r.q.QueryRow(ctx, `SELECT
			COALESCE(SUM(amount_minor) FILTER (WHERE direction = 'CREDIT'), 0)::bigint,
			COALESCE(SUM(amount_minor) FILTER (WHERE direction = 'DEBIT'), 0)::bigint,
			COUNT(*)
		FROM wallet_ledger_entries WHERE wallet_id = $1`, walletID).Scan(&t.CreditsMinor, &t.DebitsMinor, &t.Entries)
	return t, err
}
