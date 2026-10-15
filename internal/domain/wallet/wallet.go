// Package wallet contains the Wallet aggregate root and its ledger entries.
package wallet

import (
	"time"

	"github.com/google/uuid"

	"github.com/julioCAlmeida/backend-challenge-go/internal/domain/derr"
	"github.com/julioCAlmeida/backend-challenge-go/internal/domain/money"
)

var (
	ErrInvalidWallet     = derr.New(derr.KindValidation, "INVALID_WALLET", "wallet data is invalid")
	ErrInsufficientFunds = derr.New(derr.KindRejected, "INSUFFICIENT_FUNDS", "wallet balance is not enough for the debit")
	ErrCurrencyMismatch  = derr.New(derr.KindRejected, "WALLET_CURRENCY_MISMATCH", "movement currency differs from the wallet currency")
	ErrInvalidMovement   = derr.New(derr.KindInvariant, "INVALID_MOVEMENT", "movement amount must be positive")
	ErrWalletNotFound    = derr.New(derr.KindNotFound, "WALLET_NOT_FOUND", "wallet not found")
	ErrWalletExists      = derr.New(derr.KindConflict, "WALLET_ALREADY_EXISTS", "a wallet already exists for this player and currency")
)

// InitialVersion is the version of a freshly opened wallet.
const InitialVersion int64 = 1

// Wallet is the financial aggregate root. Its balance only changes through
// Debit/Credit, which always produce the matching LedgerEntry; the caller must
// persist both in the same SQL transaction.
type Wallet struct {
	id        uuid.UUID
	playerID  uuid.UUID
	currency  money.Currency
	balance   money.Money
	version   int64
	createdAt time.Time
	updatedAt time.Time

	// persistedVersion is the version read from storage; repositories use it for
	// the conditional update (optimistic check on top of the row lock).
	persistedVersion int64
	isNew            bool
}

// Snapshot is the persisted shape of a wallet.
type Snapshot struct {
	ID        uuid.UUID
	PlayerID  uuid.UUID
	Balance   money.Money
	Version   int64
	CreatedAt time.Time
	UpdatedAt time.Time
}

// OpenParams carries everything needed to open a wallet.
type OpenParams struct {
	WalletID       uuid.UUID
	PlayerID       uuid.UUID
	InitialBalance money.Money
	// OpeningTransactionID and LedgerEntryID are only used when InitialBalance > 0.
	OpeningTransactionID uuid.UUID
	LedgerEntryID        uuid.UUID
	Now                  time.Time
}

// Open creates a wallet at version 1. A positive initial balance produces the
// opening credit entry (balance 0 -> initial) without bumping the version; a zero
// initial balance produces no entry.
func Open(p OpenParams) (*Wallet, *LedgerEntry, error) {
	if p.WalletID == uuid.Nil || p.PlayerID == uuid.Nil {
		return nil, nil, derr.Wrapf(ErrInvalidWallet, "wallet and player ids are required")
	}
	if p.Now.IsZero() {
		return nil, nil, derr.Wrapf(ErrInvalidWallet, "timestamp is required")
	}
	if err := p.InitialBalance.Validate(); err != nil {
		return nil, nil, err
	}
	if p.InitialBalance.IsNegative() {
		return nil, nil, money.ErrNegativeAmount
	}
	now := p.Now.UTC()
	w := &Wallet{
		id: p.WalletID, playerID: p.PlayerID, currency: p.InitialBalance.Currency(),
		balance: p.InitialBalance, version: InitialVersion, createdAt: now, updatedAt: now, isNew: true,
	}
	if p.InitialBalance.IsZero() {
		return w, nil, nil
	}
	zero, _ := money.Zero(w.currency)
	entry, err := NewLedgerEntry(LedgerEntrySnapshot{
		ID: p.LedgerEntryID, WalletID: w.id, TransactionID: p.OpeningTransactionID, Direction: Credit,
		Amount: p.InitialBalance, BalanceBefore: zero, BalanceAfter: p.InitialBalance,
		WalletVersion: InitialVersion, CreatedAt: now,
	})
	if err != nil {
		return nil, nil, err
	}
	return w, &entry, nil
}

// Rehydrate rebuilds a wallet from storage without applying any movement.
func Rehydrate(s Snapshot) (*Wallet, error) {
	if s.ID == uuid.Nil || s.PlayerID == uuid.Nil {
		return nil, derr.Wrapf(ErrInvalidWallet, "wallet and player ids are required")
	}
	if err := s.Balance.Validate(); err != nil {
		return nil, err
	}
	if s.Balance.IsNegative() {
		return nil, derr.Wrapf(ErrInvalidWallet, "persisted balance is negative")
	}
	if s.Version < InitialVersion {
		return nil, derr.Wrapf(ErrInvalidWallet, "persisted version %d is invalid", s.Version)
	}
	return &Wallet{
		id: s.ID, playerID: s.PlayerID, currency: s.Balance.Currency(), balance: s.Balance,
		version: s.Version, createdAt: s.CreatedAt.UTC(), updatedAt: s.UpdatedAt.UTC(),
		persistedVersion: s.Version,
	}, nil
}

func (w *Wallet) ID() uuid.UUID            { return w.id }
func (w *Wallet) PlayerID() uuid.UUID      { return w.playerID }
func (w *Wallet) Currency() money.Currency { return w.currency }
func (w *Wallet) Balance() money.Money     { return w.balance }
func (w *Wallet) Version() int64           { return w.version }
func (w *Wallet) CreatedAt() time.Time     { return w.createdAt }
func (w *Wallet) UpdatedAt() time.Time     { return w.updatedAt }

// PersistedVersion is the version loaded from storage (0 for a new wallet).
func (w *Wallet) PersistedVersion() int64 { return w.persistedVersion }

// IsNew reports whether the wallet was created by Open (not yet persisted).
func (w *Wallet) IsNew() bool { return w.isNew }

// Changed reports whether the balance changed since it was loaded.
func (w *Wallet) Changed() bool { return !w.isNew && w.version != w.persistedVersion }

// Snapshot exposes the persisted shape.
func (w *Wallet) Snapshot() Snapshot {
	return Snapshot{ID: w.id, PlayerID: w.playerID, Balance: w.balance, Version: w.version, CreatedAt: w.createdAt, UpdatedAt: w.updatedAt}
}

// Debit removes amount from the balance. The balance never goes below zero.
func (w *Wallet) Debit(amount money.Money, transactionID, entryID uuid.UUID, now time.Time) (LedgerEntry, error) {
	return w.move(Debit, amount, transactionID, entryID, now)
}

// Credit adds amount to the balance.
func (w *Wallet) Credit(amount money.Money, transactionID, entryID uuid.UUID, now time.Time) (LedgerEntry, error) {
	return w.move(Credit, amount, transactionID, entryID, now)
}

// CanDebit reports whether a debit of amount would keep the balance >= 0.
func (w *Wallet) CanDebit(amount money.Money) (bool, error) {
	if amount.Currency() != w.currency {
		return false, ErrCurrencyMismatch
	}
	c, err := w.balance.Cmp(amount)
	if err != nil {
		return false, err
	}
	return c >= 0, nil
}

func (w *Wallet) move(dir Direction, amount money.Money, transactionID, entryID uuid.UUID, now time.Time) (LedgerEntry, error) {
	if err := amount.Validate(); err != nil {
		return LedgerEntry{}, err
	}
	if amount.Currency() != w.currency {
		return LedgerEntry{}, derr.Wrapf(ErrCurrencyMismatch, "wallet is %s, movement is %s", w.currency, amount.Currency())
	}
	if !amount.IsPositive() {
		return LedgerEntry{}, ErrInvalidMovement
	}
	var after money.Money
	var err error
	if dir == Debit {
		ok, cerr := w.CanDebit(amount)
		if cerr != nil {
			return LedgerEntry{}, cerr
		}
		if !ok {
			return LedgerEntry{}, derr.Wrapf(ErrInsufficientFunds, "balance %s, debit %s", w.balance.Amount(), amount.Amount())
		}
		after, err = w.balance.Sub(amount)
	} else {
		after, err = w.balance.Add(amount)
	}
	if err != nil {
		return LedgerEntry{}, err
	}
	entry, err := NewLedgerEntry(LedgerEntrySnapshot{
		ID: entryID, WalletID: w.id, TransactionID: transactionID, Direction: dir, Amount: amount,
		BalanceBefore: w.balance, BalanceAfter: after, WalletVersion: w.version + 1, CreatedAt: now,
	})
	if err != nil {
		return LedgerEntry{}, err
	}
	w.balance = after
	w.version++
	w.updatedAt = now.UTC()
	return entry, nil
}
