package app_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/julioCAlmeida/backend-challenge-go/internal/app"
	"github.com/julioCAlmeida/backend-challenge-go/internal/domain/events"
	"github.com/julioCAlmeida/backend-challenge-go/internal/domain/money"
	"github.com/julioCAlmeida/backend-challenge-go/internal/domain/wager"
	"github.com/julioCAlmeida/backend-challenge-go/internal/domain/wallet"
)

type env struct {
	t        *testing.T
	store    *memStore
	wallets  *app.WalletService
	wagering *app.WageringService
}

func newEnv(t *testing.T) *env {
	store := newMemStore()
	log := slog.New(slog.NewJSONHandler(io.Discard, nil))
	sched := app.ExponentialSchedule{Base: time.Millisecond, Max: time.Millisecond, MaxAttempts: 2}
	return &env{
		t: t, store: store,
		wallets:  app.NewWalletService(store, app.SystemClock{}, app.UUIDv7{}, app.NopMetrics{}, log),
		wagering: app.NewWageringService(store, app.SystemClock{}, app.UUIDv7{}, app.NopMetrics{}, log, app.WageringConfig{ConflictRetries: 3, Schedule: sched}),
	}
}

func brl(t *testing.T, s string) money.Money {
	t.Helper()
	m, err := money.Parse(s, money.BRL)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func (e *env) open(balance string) wallet.Snapshot {
	e.t.Helper()
	w, err := e.wallets.Open(context.Background(), app.OpenWalletCommand{PlayerID: uuid.New(), InitialBalance: brl(e.t, balance)})
	if err != nil {
		e.t.Fatal(err)
	}
	return w
}

func req(t *testing.T, w wallet.Snapshot, kind wager.Kind, ext, amount, ref string) wager.Request {
	return wager.Request{ProviderID: "provider-a", ExternalTransactionID: ext, IdempotencyKey: "provider-a:" + ext,
		PlayerID: w.PlayerID, WalletID: w.ID, RoundID: "round-1", GameID: "g", Kind: kind, Money: brl(t, amount),
		ReferenceExternalTransactionID: ref}
}

func (e *env) process(r wager.Request) (app.ProcessResult, error) {
	return e.wagering.Process(context.Background(), app.ProcessCommand{Request: r, Channel: app.ChannelHTTP, Meta: events.Meta{CorrelationID: "c"}})
}

func TestOpenWalletWithBalanceRecordsOpeningAndEvents(t *testing.T) {
	e := newEnv(t)
	w := e.open("1000.00")
	if w.Version != 1 || w.Balance.Amount() != "1000.00" {
		t.Fatalf("wallet %+v", w)
	}
	var opening *wager.Snapshot
	for _, s := range e.store.txs {
		if s.Kind == wager.KindOpening {
			opening = &s
		}
	}
	if opening == nil || opening.Status != wager.StatusProcessed || opening.Origin != wager.OriginInternal {
		t.Fatalf("opening %+v", opening)
	}
	if len(e.store.ledger) != 1 || len(e.store.outbox) != 2 {
		t.Fatalf("ledger %d outbox %d", len(e.store.ledger), len(e.store.outbox))
	}
	if _, err := e.wallets.Open(context.Background(), app.OpenWalletCommand{PlayerID: w.PlayerID, InitialBalance: brl(t, "1.00")}); !errors.Is(err, wallet.ErrWalletExists) {
		t.Fatalf("duplicate wallet: %v", err)
	}
}

func TestOpenWalletWithZeroBalanceHasNoFinancialRecords(t *testing.T) {
	e := newEnv(t)
	e.open("0.00")
	if len(e.store.txs) != 0 || len(e.store.ledger) != 0 || len(e.store.outbox) != 0 {
		t.Fatalf("txs %d ledger %d outbox %d", len(e.store.txs), len(e.store.ledger), len(e.store.outbox))
	}
}

func TestIdempotentReplayReturnsOriginalBalance(t *testing.T) {
	e := newEnv(t)
	w := e.open("1000.00")
	first, err := e.process(req(t, w, wager.KindBet, "t-1", "25.00", ""))
	if err != nil || first.Replay || first.Transaction.ResultBalance.Amount() != "975.00" {
		t.Fatalf("first: %+v %v", first, err)
	}
	if _, err := e.process(req(t, w, wager.KindWin, "t-2", "100.00", "")); err != nil {
		t.Fatal(err)
	}
	replay, err := e.process(req(t, w, wager.KindBet, "t-1", "25.00", ""))
	if err != nil || !replay.Replay || replay.Transaction.ID != first.Transaction.ID {
		t.Fatalf("replay: %+v %v", replay, err)
	}
	if replay.Transaction.ResultBalance.Amount() != "975.00" {
		t.Fatalf("replay must return the balance observed originally, got %s", replay.Transaction.ResultBalance.Amount())
	}
	if n := len(e.store.ledger); n != 3 {
		t.Fatalf("ledger entries %d, want 3 (opening, bet, win)", n)
	}
}

func TestSameKeyDifferentPayloadConflicts(t *testing.T) {
	e := newEnv(t)
	w := e.open("100.00")
	if _, err := e.process(req(t, w, wager.KindBet, "t-1", "25.00", "")); err != nil {
		t.Fatal(err)
	}
	_, err := e.process(req(t, w, wager.KindBet, "t-1", "26.00", ""))
	if !errors.Is(err, wager.ErrIdempotencyConflict) {
		t.Fatalf("err = %v, want idempotency conflict", err)
	}
	// Same external id with another key cannot be reapplied.
	r := req(t, w, wager.KindBet, "t-1", "25.00", "")
	r.IdempotencyKey = "other-key"
	if _, err := e.process(r); !errors.Is(err, wager.ErrExternalIDConflict) {
		t.Fatalf("err = %v, want external id conflict", err)
	}
	if w2, _ := e.wallets.Get(context.Background(), w.ID); w2.Balance.Amount() != "75.00" {
		t.Fatalf("balance %s", w2.Balance.Amount())
	}
}

func TestInboxDeduplicatesAndDetectsHashMismatch(t *testing.T) {
	e := newEnv(t)
	w := e.open("100.00")
	cmd := app.ProcessCommand{Request: req(t, w, wager.KindBet, "t-1", "10.00", ""), Channel: app.ChannelSQS,
		Inbox: &app.InboxMessage{Consumer: "c", MessageID: "msg-1", PayloadHash: "h1", ReceivedAt: time.Now()}}
	first, err := e.wagering.Process(context.Background(), cmd)
	if err != nil || first.DuplicateMessage {
		t.Fatalf("first: %+v %v", first, err)
	}
	again, err := e.wagering.Process(context.Background(), cmd)
	if err != nil || !again.DuplicateMessage || !again.Replay {
		t.Fatalf("again: %+v %v", again, err)
	}
	cmd.Inbox = &app.InboxMessage{Consumer: "c", MessageID: "msg-1", PayloadHash: "different", ReceivedAt: time.Now()}
	if _, err := e.wagering.Process(context.Background(), cmd); !errors.Is(err, app.ErrInboxHashMismatch) {
		t.Fatalf("hash mismatch: %v", err)
	}
	if len(e.store.ledger) != 2 {
		t.Fatalf("ledger entries %d", len(e.store.ledger))
	}
}

func TestOpeningKindAndWalletNotFound(t *testing.T) {
	e := newEnv(t)
	w := e.open("10.00")
	r := req(t, w, wager.KindOpening, "o-1", "1.00", "")
	if _, err := e.process(r); !errors.Is(err, wager.ErrKindNotAllowed) {
		t.Fatalf("OPENING: %v", err)
	}
	r = req(t, w, wager.KindBet, "b-1", "1.00", "")
	r.WalletID = uuid.New()
	if _, err := e.process(r); !errors.Is(err, wallet.ErrWalletNotFound) {
		t.Fatalf("unknown wallet: %v", err)
	}
	if len(e.store.txs) != 1 { // only the opening
		t.Fatalf("nothing must be persisted for invalid input, txs=%d", len(e.store.txs))
	}
}

func TestPendingReferenceResolvedByWorker(t *testing.T) {
	e := newEnv(t)
	w := e.open("100.00")
	res, err := e.process(req(t, w, wager.KindRefund, "r-1", "10.00", "b-1"))
	if err != nil || res.Transaction.Status != wager.StatusPendingReference {
		t.Fatalf("refund: %+v %v", res.Transaction.Status, err)
	}
	if _, err := e.process(req(t, w, wager.KindBet, "b-1", "10.00", "")); err != nil {
		t.Fatal(err)
	}
	if _, err := e.wagering.ResumePending(context.Background(), 10); err != nil {
		t.Fatal(err)
	}
	got, _ := e.wagering.GetTransaction(context.Background(), res.Transaction.ID)
	if got.Status != wager.StatusProcessed {
		t.Fatalf("status %s", got.Status)
	}
	rec, err := e.wallets.Reconcile(context.Background(), w.ID)
	if err != nil || !rec.Consistent || rec.StoredBalance.Amount() != "100.00" || rec.CheckedEntries != 3 {
		t.Fatalf("reconcile %+v %v", rec, err)
	}
}

func TestPendingReferenceExpiresToRejected(t *testing.T) {
	e := newEnv(t)
	w := e.open("100.00")
	res, _ := e.process(req(t, w, wager.KindRollback, "rb-1", "10.00", "missing"))
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		_, _ = e.wagering.ResumePending(context.Background(), 10)
		got, _ := e.wagering.GetTransaction(context.Background(), res.Transaction.ID)
		if got.Status == wager.StatusRejected {
			if got.FailureCode != wager.FailureReferenceNotFound {
				t.Fatalf("code %s", got.FailureCode)
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("pending reference never expired")
}

func TestLedgerCursor(t *testing.T) {
	e := newEnv(t)
	w := e.open("100.00")
	for i := 0; i < 5; i++ {
		if _, err := e.process(req(t, w, wager.KindWin, uuid.NewString(), "1.00", "")); err != nil {
			t.Fatal(err)
		}
	}
	page, err := e.wallets.Ledger(context.Background(), w.ID, "", 4)
	if err != nil || len(page.Entries) != 4 || page.NextCursor == "" {
		t.Fatalf("page1 %d %q %v", len(page.Entries), page.NextCursor, err)
	}
	page2, err := e.wallets.Ledger(context.Background(), w.ID, page.NextCursor, 4)
	if err != nil || len(page2.Entries) != 2 || page2.NextCursor != "" {
		t.Fatalf("page2 %d %q %v", len(page2.Entries), page2.NextCursor, err)
	}
	if _, err := e.wallets.Ledger(context.Background(), w.ID, "not-a-cursor", 4); !errors.Is(err, app.ErrInvalidCursor) {
		t.Fatalf("bad cursor: %v", err)
	}
}

func TestBackoffAndSchedule(t *testing.T) {
	if d := app.Backoff(time.Second, time.Minute, 0); d != time.Second {
		t.Errorf("attempt 0: %v", d)
	}
	if d := app.Backoff(time.Second, time.Minute, 3); d != 8*time.Second {
		t.Errorf("attempt 3: %v", d)
	}
	if d := app.Backoff(time.Second, time.Minute, 30); d != time.Minute {
		t.Errorf("capped: %v", d)
	}
	s := app.ExponentialSchedule{Base: time.Second, Max: time.Minute, MaxAttempts: 3, TTL: time.Hour}
	now := time.Now()
	if _, expired := s.Next(3, now, now); !expired {
		t.Error("max attempts must expire")
	}
	if _, expired := s.Next(0, now.Add(-2*time.Hour), now); !expired {
		t.Error("ttl must expire")
	}
	if next, expired := s.Next(1, now, now); expired || next.Sub(now) != 2*time.Second {
		t.Errorf("next %v %v", next.Sub(now), expired)
	}
}
