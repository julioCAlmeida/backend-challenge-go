package wallet

import (
	"time"

	"github.com/google/uuid"

	"github.com/julioCAlmeida/backend-challenge-go/internal/domain/derr"
	"github.com/julioCAlmeida/backend-challenge-go/internal/domain/money"
)

// Direction of a ledger entry from the wallet's point of view.
type Direction string

const (
	Debit  Direction = "DEBIT"
	Credit Direction = "CREDIT"
)

var ErrInvalidLedgerEntry = derr.New(derr.KindInvariant, "INVALID_LEDGER_ENTRY", "ledger entry is inconsistent")

// LedgerEntry is an immutable, append-only record of one balance movement.
// It has no setters: corrections require new entries.
type LedgerEntry struct {
	id            uuid.UUID
	walletID      uuid.UUID
	transactionID uuid.UUID
	direction     Direction
	amount        money.Money
	balanceBefore money.Money
	balanceAfter  money.Money
	walletVersion int64
	createdAt     time.Time
}

// LedgerEntrySnapshot is the persisted shape used for rehydration.
type LedgerEntrySnapshot struct {
	ID            uuid.UUID
	WalletID      uuid.UUID
	TransactionID uuid.UUID
	Direction     Direction
	Amount        money.Money
	BalanceBefore money.Money
	BalanceAfter  money.Money
	WalletVersion int64
	CreatedAt     time.Time
}

// NewLedgerEntry validates balanceAfter = balanceBefore ± amount according to the
// direction, a strictly positive amount, matching currencies and non-negative
// balances.
func NewLedgerEntry(s LedgerEntrySnapshot) (LedgerEntry, error) {
	if s.ID == uuid.Nil || s.WalletID == uuid.Nil || s.TransactionID == uuid.Nil {
		return LedgerEntry{}, derr.Wrapf(ErrInvalidLedgerEntry, "identifiers are required")
	}
	if s.WalletVersion < 1 {
		return LedgerEntry{}, derr.Wrapf(ErrInvalidLedgerEntry, "wallet version must be >= 1")
	}
	if s.CreatedAt.IsZero() {
		return LedgerEntry{}, derr.Wrapf(ErrInvalidLedgerEntry, "createdAt is required")
	}
	for _, m := range []money.Money{s.Amount, s.BalanceBefore, s.BalanceAfter} {
		if err := m.Validate(); err != nil {
			return LedgerEntry{}, err
		}
		if m.Currency() != s.Amount.Currency() {
			return LedgerEntry{}, derr.Wrapf(ErrInvalidLedgerEntry, "currencies differ")
		}
	}
	if !s.Amount.IsPositive() {
		return LedgerEntry{}, derr.Wrapf(ErrInvalidLedgerEntry, "amount must be positive")
	}
	if s.BalanceBefore.IsNegative() || s.BalanceAfter.IsNegative() {
		return LedgerEntry{}, derr.Wrapf(ErrInvalidLedgerEntry, "balances must not be negative")
	}
	var expected money.Money
	var err error
	switch s.Direction {
	case Credit:
		expected, err = s.BalanceBefore.Add(s.Amount)
	case Debit:
		expected, err = s.BalanceBefore.Sub(s.Amount)
	default:
		return LedgerEntry{}, derr.Wrapf(ErrInvalidLedgerEntry, "unknown direction %q", s.Direction)
	}
	if err != nil {
		return LedgerEntry{}, err
	}
	if !expected.Equal(s.BalanceAfter) {
		return LedgerEntry{}, derr.Wrapf(ErrInvalidLedgerEntry, "balanceAfter %s != balanceBefore %s %s %s",
			s.BalanceAfter.Amount(), s.BalanceBefore.Amount(), s.Direction, s.Amount.Amount())
	}
	return LedgerEntry{
		id: s.ID, walletID: s.WalletID, transactionID: s.TransactionID, direction: s.Direction,
		amount: s.Amount, balanceBefore: s.BalanceBefore, balanceAfter: s.BalanceAfter,
		walletVersion: s.WalletVersion, createdAt: s.CreatedAt.UTC(),
	}, nil
}

func (e LedgerEntry) ID() uuid.UUID              { return e.id }
func (e LedgerEntry) WalletID() uuid.UUID        { return e.walletID }
func (e LedgerEntry) TransactionID() uuid.UUID   { return e.transactionID }
func (e LedgerEntry) Direction() Direction       { return e.direction }
func (e LedgerEntry) Amount() money.Money        { return e.amount }
func (e LedgerEntry) BalanceBefore() money.Money { return e.balanceBefore }
func (e LedgerEntry) BalanceAfter() money.Money  { return e.balanceAfter }
func (e LedgerEntry) WalletVersion() int64       { return e.walletVersion }
func (e LedgerEntry) CreatedAt() time.Time       { return e.createdAt }

// Snapshot exposes the persisted shape.
func (e LedgerEntry) Snapshot() LedgerEntrySnapshot {
	return LedgerEntrySnapshot{
		ID: e.id, WalletID: e.walletID, TransactionID: e.transactionID, Direction: e.direction,
		Amount: e.amount, BalanceBefore: e.balanceBefore, BalanceAfter: e.balanceAfter,
		WalletVersion: e.walletVersion, CreatedAt: e.createdAt,
	}
}

// SignedAmount returns +amount for credits and -amount for debits.
func (e LedgerEntry) SignedAmount() (money.Money, error) {
	if e.direction == Debit {
		return e.amount.Negate()
	}
	return e.amount, nil
}
