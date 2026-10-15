package app

import (
	"context"
	"encoding/base64"
	"log/slog"
	"strconv"
	"strings"

	"github.com/google/uuid"

	"github.com/julioCAlmeida/backend-challenge-go/internal/domain/derr"
	"github.com/julioCAlmeida/backend-challenge-go/internal/domain/events"
	"github.com/julioCAlmeida/backend-challenge-go/internal/domain/money"
	"github.com/julioCAlmeida/backend-challenge-go/internal/domain/wager"
	"github.com/julioCAlmeida/backend-challenge-go/internal/domain/wallet"
)

// WalletService implements wallet opening, reads and reconciliation.
type WalletService struct {
	uow     UnitOfWork
	clock   Clock
	ids     IDGenerator
	metrics Metrics
	log     *slog.Logger
}

func NewWalletService(uow UnitOfWork, clock Clock, ids IDGenerator, metrics Metrics, log *slog.Logger) *WalletService {
	return &WalletService{uow: uow, clock: clock, ids: ids, metrics: metrics, log: log}
}

// OpenWalletCommand opens a wallet for a player.
type OpenWalletCommand struct {
	PlayerID       uuid.UUID
	InitialBalance money.Money
	Meta           events.Meta
}

// Open creates the wallet. A positive initial balance creates, in the same
// commit, the PROCESSED OPENING transaction, its credit entry and the outbox
// records of WagerTransactionProcessed and WalletBalanceChanged.
func (s *WalletService) Open(ctx context.Context, cmd OpenWalletCommand) (wallet.Snapshot, error) {
	now := s.clock.Now()
	walletID, openingID := s.ids.New(), s.ids.New()
	w, entry, err := wallet.Open(wallet.OpenParams{
		WalletID: walletID, PlayerID: cmd.PlayerID, InitialBalance: cmd.InitialBalance,
		OpeningTransactionID: openingID, LedgerEntryID: s.ids.New(), Now: now,
	})
	if err != nil {
		return wallet.Snapshot{}, err
	}
	err = s.uow.Within(ctx, func(ctx context.Context, tx Tx) error {
		if err := tx.Wallets().Insert(ctx, w); err != nil {
			return err
		}
		if entry == nil {
			return nil
		}
		opening, err := wager.NewOpening(openingID, w.ID(), w.PlayerID(), cmd.InitialBalance, cmd.Meta, now)
		if err != nil {
			return err
		}
		if err := opening.CompleteOpening(*entry, now); err != nil {
			return err
		}
		if err := tx.Transactions().Insert(ctx, opening); err != nil {
			return err
		}
		if err := tx.Ledger().Append(ctx, *entry); err != nil {
			return err
		}
		return tx.Outbox().Append(ctx, opening.PullEvents()...)
	})
	if err != nil {
		return wallet.Snapshot{}, err
	}
	s.log.InfoContext(ctx, "wallet opened", slog.String("walletId", w.ID().String()), slog.String("currency", w.Currency().Code()))
	return w.Snapshot(), nil
}

// Get returns a wallet.
func (s *WalletService) Get(ctx context.Context, id uuid.UUID) (wallet.Snapshot, error) {
	var snap wallet.Snapshot
	err := s.uow.Within(ctx, func(ctx context.Context, tx Tx) error {
		w, err := tx.Wallets().Get(ctx, id)
		if err != nil {
			return err
		}
		snap = w.Snapshot()
		return nil
	})
	return snap, err
}

// LedgerPage is a page of ledger entries ordered by their append sequence.
type LedgerPage struct {
	Entries    []wallet.LedgerEntrySnapshot
	NextCursor string
}

const (
	DefaultLedgerLimit = 50
	MaxLedgerLimit     = 200
	cursorPrefix       = "v1:"
)

// EncodeCursor makes the opaque cursor for the position after seq.
func EncodeCursor(seq int64) string {
	return base64.RawURLEncoding.EncodeToString([]byte(cursorPrefix + strconv.FormatInt(seq, 10)))
}

// DecodeCursor parses a cursor produced by EncodeCursor ("" means the beginning).
func DecodeCursor(c string) (int64, error) {
	if c == "" {
		return 0, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(c)
	if err != nil || !strings.HasPrefix(string(raw), cursorPrefix) {
		return 0, ErrInvalidCursor
	}
	seq, err := strconv.ParseInt(strings.TrimPrefix(string(raw), cursorPrefix), 10, 64)
	if err != nil || seq < 0 {
		return 0, ErrInvalidCursor
	}
	return seq, nil
}

// Ledger lists entries of a wallet with keyset pagination on the append sequence.
func (s *WalletService) Ledger(ctx context.Context, walletID uuid.UUID, cursor string, limit int) (LedgerPage, error) {
	after, err := DecodeCursor(cursor)
	if err != nil {
		return LedgerPage{}, err
	}
	if limit <= 0 {
		limit = DefaultLedgerLimit
	}
	if limit > MaxLedgerLimit {
		return LedgerPage{}, derr.Wrapf(ErrInvalidCursor, "limit must be between 1 and %d", MaxLedgerLimit)
	}
	var page LedgerPage
	err = s.uow.Within(ctx, func(ctx context.Context, tx Tx) error {
		if _, err := tx.Wallets().Get(ctx, walletID); err != nil {
			return err
		}
		rows, err := tx.Ledger().List(ctx, walletID, after, limit+1)
		if err != nil {
			return err
		}
		if len(rows) > limit {
			rows = rows[:limit]
			page.NextCursor = EncodeCursor(rows[len(rows)-1].Seq)
		}
		page.Entries = make([]wallet.LedgerEntrySnapshot, 0, len(rows))
		for _, r := range rows {
			page.Entries = append(page.Entries, r.Entry.Snapshot())
		}
		return nil
	})
	return page, err
}

// Reconciliation compares the stored balance with the ledger reconstruction.
type Reconciliation struct {
	WalletID          uuid.UUID
	StoredBalance     money.Money
	CalculatedBalance money.Money
	Difference        money.Money
	Consistent        bool
	CheckedEntries    int64
}

// Reconcile rebuilds the balance from the ledger (opening included) inside one
// REPEATABLE READ snapshot. It never changes the balance.
func (s *WalletService) Reconcile(ctx context.Context, walletID uuid.UUID) (Reconciliation, error) {
	var r Reconciliation
	err := s.uow.Snapshot(ctx, func(ctx context.Context, tx Tx) error {
		w, err := tx.Wallets().Get(ctx, walletID)
		if err != nil {
			return err
		}
		totals, err := tx.Ledger().Totals(ctx, walletID)
		if err != nil {
			return err
		}
		credits, err := money.FromMinor(totals.CreditsMinor, w.Currency())
		if err != nil {
			return err
		}
		debits, err := money.FromMinor(totals.DebitsMinor, w.Currency())
		if err != nil {
			return err
		}
		calculated, err := credits.Sub(debits)
		if err != nil {
			return err
		}
		diff, err := w.Balance().Sub(calculated)
		if err != nil {
			return err
		}
		r = Reconciliation{
			WalletID: walletID, StoredBalance: w.Balance(), CalculatedBalance: calculated,
			Difference: diff, Consistent: diff.IsZero(), CheckedEntries: totals.Entries,
		}
		return nil
	})
	if err != nil {
		return Reconciliation{}, err
	}
	s.metrics.ReconciliationChecked(r.Consistent)
	if !r.Consistent {
		s.log.ErrorContext(ctx, "reconciliation divergence",
			slog.String("walletId", walletID.String()),
			slog.String("storedBalance", r.StoredBalance.Amount()),
			slog.String("calculatedBalance", r.CalculatedBalance.Amount()),
			slog.String("difference", r.Difference.Amount()))
	} else {
		s.log.InfoContext(ctx, "reconciliation consistent", slog.String("walletId", walletID.String()), slog.Int64("checkedEntries", r.CheckedEntries))
	}
	return r, nil
}
