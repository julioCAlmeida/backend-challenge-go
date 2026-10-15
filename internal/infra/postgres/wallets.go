package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/julioCAlmeida/backend-challenge-go/internal/app"
	"github.com/julioCAlmeida/backend-challenge-go/internal/domain/money"
	"github.com/julioCAlmeida/backend-challenge-go/internal/domain/wallet"
)

type walletRepo struct{ q querier }

const walletColumns = `id, player_id, currency, balance_minor, version, created_at, updated_at`

func (r walletRepo) Insert(ctx context.Context, w *wallet.Wallet) error {
	_, err := r.q.Exec(ctx, `INSERT INTO wallets (`+walletColumns+`) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		w.ID(), w.PlayerID(), w.Currency().Code(), w.Balance().MinorUnits(), w.Version(), w.CreatedAt(), w.UpdatedAt())
	if isUniqueViolation(err, "wallets_player_currency_uk") {
		return wallet.ErrWalletExists
	}
	return err
}

func (r walletRepo) Get(ctx context.Context, id uuid.UUID) (*wallet.Wallet, error) {
	return r.one(ctx, `SELECT `+walletColumns+` FROM wallets WHERE id = $1`, id)
}

func (r walletRepo) GetForUpdate(ctx context.Context, id uuid.UUID) (*wallet.Wallet, error) {
	return r.one(ctx, `SELECT `+walletColumns+` FROM wallets WHERE id = $1 FOR UPDATE`, id)
}

func (r walletRepo) TryGetForUpdate(ctx context.Context, id uuid.UUID) (*wallet.Wallet, bool, error) {
	w, err := r.one(ctx, `SELECT `+walletColumns+` FROM wallets WHERE id = $1 FOR UPDATE SKIP LOCKED`, id)
	if errors.Is(err, wallet.ErrWalletNotFound) {
		// The wallet exists (pending rows reference it): it is locked by someone else.
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return w, true, nil
}

// UpdateBalance is a conditional update on the version read under the row lock.
// Zero affected rows means another writer committed first: the caller retries
// instead of overwriting a confirmed update.
func (r walletRepo) UpdateBalance(ctx context.Context, w *wallet.Wallet) error {
	tag, err := r.q.Exec(ctx,
		`UPDATE wallets SET balance_minor = $1, version = $2, updated_at = $3 WHERE id = $4 AND version = $5`,
		w.Balance().MinorUnits(), w.Version(), w.UpdatedAt(), w.ID(), w.PersistedVersion())
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%w: wallet %s version %d changed", app.ErrConcurrencyConflict, w.ID(), w.PersistedVersion())
	}
	return nil
}

func (r walletRepo) one(ctx context.Context, sql string, args ...any) (*wallet.Wallet, error) {
	var (
		id, player       uuid.UUID
		currency         string
		balance, version int64
		created, updated time.Time
	)
	err := r.q.QueryRow(ctx, sql, args...).Scan(&id, &player, &currency, &balance, &version, &created, &updated)
	if notFound(err) {
		return nil, wallet.ErrWalletNotFound
	}
	if err != nil {
		return nil, err
	}
	m, err := toMoney(balance, currency)
	if err != nil {
		return nil, err
	}
	return wallet.Rehydrate(wallet.Snapshot{ID: id, PlayerID: player, Balance: m, Version: version, CreatedAt: created, UpdatedAt: updated})
}

func toMoney(minor int64, currency string) (money.Money, error) {
	c, err := money.ParseCurrency(strings.TrimSpace(currency))
	if err != nil {
		return money.Money{}, err
	}
	return money.FromMinor(minor, c)
}
