package app_test

import (
	"context"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/julioCAlmeida/backend-challenge-go/internal/app"
	"github.com/julioCAlmeida/backend-challenge-go/internal/domain/events"
	"github.com/julioCAlmeida/backend-challenge-go/internal/domain/wager"
	"github.com/julioCAlmeida/backend-challenge-go/internal/domain/wallet"
)

// memStore is an in-memory UnitOfWork for unit tests. One global mutex
// serializes transactions; a failed transaction restores the previous state.
// The real concurrency guarantees are verified against PostgreSQL in the
// integration tests.
type memStore struct {
	mu      sync.Mutex
	wallets map[uuid.UUID]wallet.Snapshot
	txs     map[uuid.UUID]wager.Snapshot
	ledger  []wallet.LedgerEntrySnapshot
	outbox  []events.Event
	inbox   map[string]app.InboxRecord
}

func newMemStore() *memStore {
	return &memStore{wallets: map[uuid.UUID]wallet.Snapshot{}, txs: map[uuid.UUID]wager.Snapshot{}, inbox: map[string]app.InboxRecord{}}
}

func (s *memStore) Within(ctx context.Context, fn func(ctx context.Context, tx app.Tx) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	w, t, l, o, i := maps.Clone(s.wallets), maps.Clone(s.txs), slices.Clone(s.ledger), slices.Clone(s.outbox), maps.Clone(s.inbox)
	if err := fn(ctx, memTx{s}); err != nil {
		s.wallets, s.txs, s.ledger, s.outbox, s.inbox = w, t, l, o, i
		return err
	}
	return nil
}

func (s *memStore) Snapshot(ctx context.Context, fn func(ctx context.Context, tx app.Tx) error) error {
	return s.Within(ctx, fn)
}

type memTx struct{ s *memStore }

func (t memTx) Wallets() app.WalletRepository           { return memWallets(t) }
func (t memTx) Transactions() app.TransactionRepository { return memTxs(t) }
func (t memTx) Ledger() app.LedgerRepository            { return memLedger(t) }
func (t memTx) Outbox() app.OutboxWriter                { return memOutbox(t) }
func (t memTx) Inbox() app.InboxRepository              { return memInbox(t) }

type memWallets memTx

func (r memWallets) Insert(_ context.Context, w *wallet.Wallet) error {
	for _, x := range r.s.wallets {
		if x.PlayerID == w.PlayerID() && x.Balance.Currency() == w.Currency() {
			return wallet.ErrWalletExists
		}
	}
	r.s.wallets[w.ID()] = w.Snapshot()
	return nil
}

func (r memWallets) Get(_ context.Context, id uuid.UUID) (*wallet.Wallet, error) {
	s, ok := r.s.wallets[id]
	if !ok {
		return nil, wallet.ErrWalletNotFound
	}
	return wallet.Rehydrate(s)
}

func (r memWallets) GetForUpdate(ctx context.Context, id uuid.UUID) (*wallet.Wallet, error) {
	return r.Get(ctx, id)
}

func (r memWallets) TryGetForUpdate(ctx context.Context, id uuid.UUID) (*wallet.Wallet, bool, error) {
	w, err := r.Get(ctx, id)
	return w, err == nil, err
}

func (r memWallets) UpdateBalance(_ context.Context, w *wallet.Wallet) error {
	if r.s.wallets[w.ID()].Version != w.PersistedVersion() {
		return app.ErrConcurrencyConflict
	}
	r.s.wallets[w.ID()] = w.Snapshot()
	return nil
}

type memTxs memTx

func (r memTxs) Insert(_ context.Context, t *wager.Transaction) error {
	r.s.txs[t.ID()] = t.Snapshot()
	return nil
}

func (r memTxs) InsertIfAbsent(ctx context.Context, t *wager.Transaction) (bool, error) {
	for _, x := range r.s.txs {
		if x.ProviderID == t.ProviderID() && (x.IdempotencyKey == t.IdempotencyKey() || x.ExternalTransactionID == t.ExternalTransactionID()) {
			return false, nil
		}
	}
	return true, r.Insert(ctx, t)
}

func (r memTxs) Update(_ context.Context, t *wager.Transaction) error {
	r.s.txs[t.ID()] = t.Snapshot()
	return nil
}

func (r memTxs) Get(_ context.Context, id uuid.UUID) (*wager.Transaction, error) {
	s, ok := r.s.txs[id]
	if !ok {
		return nil, wager.ErrTransactionNotFound
	}
	return wager.Rehydrate(s)
}

func (r memTxs) GetForUpdate(ctx context.Context, id uuid.UUID) (*wager.Transaction, error) {
	return r.Get(ctx, id)
}

func (r memTxs) find(match func(wager.Snapshot) bool) (*wager.Transaction, error) {
	for _, s := range r.s.txs {
		if match(s) {
			return wager.Rehydrate(s)
		}
	}
	return nil, nil
}

func (r memTxs) FindByKeyOrExternalID(_ context.Context, p, k, e string) ([]*wager.Transaction, error) {
	var out []*wager.Transaction
	for _, s := range r.s.txs {
		if s.ProviderID == p && (s.IdempotencyKey == k || s.ExternalTransactionID == e) {
			t, err := wager.Rehydrate(s)
			if err != nil {
				return nil, err
			}
			out = append(out, t)
		}
	}
	return out, nil
}

func (r memTxs) FindByExternalID(_ context.Context, p, e string) (*wager.Transaction, error) {
	return r.find(func(s wager.Snapshot) bool { return s.ProviderID == p && s.ExternalTransactionID == e })
}

func (r memTxs) HasProcessedReversal(_ context.Context, ref uuid.UUID) (bool, error) {
	for _, s := range r.s.txs {
		if s.ReferenceTransactionID != nil && *s.ReferenceTransactionID == ref && s.Status == wager.StatusProcessed &&
			(s.Kind == wager.KindRefund || s.Kind == wager.KindRollback) {
			return true, nil
		}
	}
	return false, nil
}

func (r memTxs) WakeWaiting(_ context.Context, p, e string, at time.Time) error {
	for id, s := range r.s.txs {
		if s.ProviderID == p && s.ReferenceExternalTransactionID == e && s.Status == wager.StatusPendingReference {
			s.NextAttemptAt = &at
			r.s.txs[id] = s
		}
	}
	return nil
}

func (r memTxs) DuePending(_ context.Context, now time.Time, limit int) ([]app.PendingRef, error) {
	var out []app.PendingRef
	for _, s := range r.s.txs {
		if s.Status == wager.StatusPending || (s.Status == wager.StatusPendingReference && !s.NextAttemptAt.After(now)) {
			out = append(out, app.PendingRef{TransactionID: s.ID, WalletID: s.WalletID})
		}
	}
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

type memLedger memTx

func (r memLedger) Append(_ context.Context, e wallet.LedgerEntry) error {
	r.s.ledger = append(r.s.ledger, e.Snapshot())
	return nil
}

func (r memLedger) List(_ context.Context, w uuid.UUID, after int64, limit int) ([]app.LedgerRow, error) {
	var out []app.LedgerRow
	for i, s := range r.s.ledger {
		seq := int64(i + 1)
		if s.WalletID != w || seq <= after {
			continue
		}
		e, err := wallet.NewLedgerEntry(s)
		if err != nil {
			return nil, err
		}
		out = append(out, app.LedgerRow{Seq: seq, Entry: e})
		if len(out) == limit {
			break
		}
	}
	return out, nil
}

func (r memLedger) Totals(_ context.Context, w uuid.UUID) (app.LedgerTotals, error) {
	var t app.LedgerTotals
	for _, s := range r.s.ledger {
		if s.WalletID != w {
			continue
		}
		t.Entries++
		if s.Direction == wallet.Credit {
			t.CreditsMinor += s.Amount.MinorUnits()
		} else {
			t.DebitsMinor += s.Amount.MinorUnits()
		}
	}
	return t, nil
}

type memOutbox memTx

func (r memOutbox) Append(_ context.Context, evs ...events.Event) error {
	r.s.outbox = append(r.s.outbox, evs...)
	return nil
}

type memInbox memTx

func (r memInbox) Register(_ context.Context, m app.InboxMessage) (*app.InboxRecord, error) {
	k := m.Consumer + "|" + m.MessageID
	if rec, ok := r.s.inbox[k]; ok {
		return &rec, nil
	}
	r.s.inbox[k] = app.InboxRecord{InboxMessage: m}
	return nil, nil
}

func (r memInbox) Complete(_ context.Context, c, id string, tx *uuid.UUID, outcome string, at time.Time) error {
	k := c + "|" + id
	rec := r.s.inbox[k]
	rec.TransactionID, rec.Outcome, rec.CompletedAt = tx, outcome, &at
	r.s.inbox[k] = rec
	return nil
}
