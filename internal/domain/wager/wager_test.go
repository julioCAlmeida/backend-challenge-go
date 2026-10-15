package wager_test

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/julioCAlmeida/backend-challenge-go/internal/domain/events"
	"github.com/julioCAlmeida/backend-challenge-go/internal/domain/money"
	"github.com/julioCAlmeida/backend-challenge-go/internal/domain/wager"
	"github.com/julioCAlmeida/backend-challenge-go/internal/domain/wallet"
)

var now = time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)

type schedule struct{ max int }

func (s schedule) Next(attempts int, _, now time.Time) (time.Time, bool) {
	if attempts >= s.max {
		return time.Time{}, true
	}
	return now.Add(time.Second << attempts), false
}

func amt(t *testing.T, s string) money.Money {
	t.Helper()
	v, err := money.Parse(s, money.BRL)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

type fixture struct {
	t      *testing.T
	wallet *wallet.Wallet
	player uuid.UUID
}

func newFixture(t *testing.T, balance string) *fixture {
	player := uuid.New()
	w, _, err := wallet.Open(wallet.OpenParams{WalletID: uuid.New(), PlayerID: player, InitialBalance: amt(t, balance),
		OpeningTransactionID: uuid.New(), LedgerEntryID: uuid.New(), Now: now})
	if err != nil {
		t.Fatal(err)
	}
	// Simulate a persisted wallet.
	w, err = wallet.Rehydrate(w.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	return &fixture{t: t, wallet: w, player: player}
}

func (f *fixture) req(kind wager.Kind, ext, amount, ref string) wager.Request {
	return wager.Request{
		ProviderID: "provider-a", ExternalTransactionID: ext, IdempotencyKey: "provider-a:" + ext,
		PlayerID: f.player, WalletID: f.wallet.ID(), RoundID: "round-1", GameID: "fortune-chimp",
		Kind: kind, Money: amt(f.t, amount), ReferenceExternalTransactionID: ref,
	}
}

func (f *fixture) tx(kind wager.Kind, ext, amount, ref string) *wager.Transaction {
	f.t.Helper()
	t, err := wager.NewExternal(uuid.New(), f.req(kind, ext, amount, ref), events.Meta{CorrelationID: "c-1"}, now)
	if err != nil {
		f.t.Fatalf("NewExternal: %v", err)
	}
	return t
}

func (f *fixture) exec(t *wager.Transaction, ref *wager.Transaction, reversed bool) wager.Result {
	f.t.Helper()
	res, err := t.Execute(wager.ExecuteParams{Wallet: f.wallet, Reference: wager.ReferenceState{Tx: ref, AlreadyReversed: reversed},
		LedgerEntryID: uuid.New(), Schedule: schedule{max: 3}, Now: now})
	if err != nil {
		f.t.Fatalf("Execute: %v", err)
	}
	return res
}

func eventTypes(evs []events.Event) []string {
	out := make([]string, 0, len(evs))
	for _, e := range evs {
		out = append(out, e.Type())
	}
	return out
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestRequestValidationAndZeroPolicy(t *testing.T) {
	f := newFixture(t, "100.00")
	valid := []wager.Request{
		f.req(wager.KindBet, "b1", "1.00", ""),
		f.req(wager.KindWin, "w1", "1.00", ""),
		f.req(wager.KindWin, "w2", "1.00", "b1"),
		f.req(wager.KindLoss, "l1", "0.00", ""),
		f.req(wager.KindRefund, "r1", "1.00", "b1"),
		f.req(wager.KindRollback, "rb1", "1.00", "b1"),
	}
	for _, r := range valid {
		if _, err := wager.NewRequest(r); err != nil {
			t.Errorf("%s rejected: %v", r.Kind, err)
		}
	}
	invalid := map[string]wager.Request{
		"BET zero":             f.req(wager.KindBet, "x", "0.00", ""),
		"WIN zero":             f.req(wager.KindWin, "x", "0.00", ""),
		"REFUND zero":          f.req(wager.KindRefund, "x", "0.00", "b1"),
		"ROLLBACK zero":        f.req(wager.KindRollback, "x", "0.00", "b1"),
		"LOSS positive":        f.req(wager.KindLoss, "x", "1.00", ""),
		"REFUND without ref":   f.req(wager.KindRefund, "x", "1.00", ""),
		"ROLLBACK without ref": f.req(wager.KindRollback, "x", "1.00", ""),
		"BET with ref":         f.req(wager.KindBet, "x", "1.00", "b1"),
		"LOSS with ref":        f.req(wager.KindLoss, "x", "0.00", "b1"),
		"OPENING":              f.req(wager.KindOpening, "x", "1.00", ""),
		"self reference":       f.req(wager.KindRefund, "x", "1.00", "x"),
	}
	for name, r := range invalid {
		if _, err := wager.NewRequest(r); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
	r := f.req(wager.KindBet, "x", "1.00", "")
	r.ProviderID = ""
	if _, err := wager.NewRequest(r); err == nil {
		t.Error("missing provider accepted")
	}
	r = f.req(wager.KindBet, "x", "1.00", "")
	r.RoundID = "round 1"
	if _, err := wager.NewRequest(r); err == nil {
		t.Error("whitespace identifier accepted")
	}
	r = f.req(wager.KindBet, "x", "1.00", "")
	r.Money = money.Money{}
	if _, err := wager.NewRequest(r); err == nil {
		t.Error("uninitialized money accepted")
	}
	neg, _ := money.Parse("-1.00", money.BRL)
	r.Money = neg
	if _, err := wager.NewRequest(r); !errors.Is(err, money.ErrNegativeAmount) {
		t.Errorf("negative money: %v", err)
	}
}

func TestParseExternalKind(t *testing.T) {
	if _, err := wager.ParseExternalKind("OPENING"); !errors.Is(err, wager.ErrKindNotAllowed) {
		t.Errorf("OPENING: %v", err)
	}
	if _, err := wager.ParseExternalKind("bet"); !errors.Is(err, wager.ErrInvalidKind) {
		t.Errorf("lowercase: %v", err)
	}
}

func TestPayloadHashIsDeterministicAndExcludesKey(t *testing.T) {
	f := newFixture(t, "1.00")
	a := f.req(wager.KindBet, "t-1", "25.00", "")
	b := a
	b.IdempotencyKey = "another-key"
	if a.PayloadHash() != b.PayloadHash() {
		t.Error("idempotency key must not be part of the hash")
	}
	c := a
	c.Money = amt(t, "25.01")
	if a.PayloadHash() == c.PayloadHash() {
		t.Error("amount change must change the hash")
	}
	d := a
	d.GameID = "other"
	if a.PayloadHash() == d.PayloadHash() {
		t.Error("game change must change the hash")
	}
	if len(a.PayloadHash()) != 64 {
		t.Errorf("hash length %d", len(a.PayloadHash()))
	}
}

func TestBetDebitsAndRejectsInsufficientFunds(t *testing.T) {
	f := newFixture(t, "100.00")
	bet := f.tx(wager.KindBet, "b1", "80.00", "")
	res := f.exec(bet, nil, false)
	if bet.Status() != wager.StatusProcessed || res.Entry == nil || res.Entry.Direction() != wallet.Debit {
		t.Fatalf("bet: %s %v", bet.Status(), res.Entry)
	}
	if bet.ResultBalance().Amount() != "20.00" || f.wallet.Balance().Amount() != "20.00" {
		t.Fatalf("balance %s", f.wallet.Balance())
	}
	if got := eventTypes(bet.PullEvents()); !equal(got, []string{events.TypeWagerTransactionProcessed, events.TypeWalletBalanceChanged}) {
		t.Fatalf("events %v", got)
	}
	second := f.tx(wager.KindBet, "b2", "80.00", "")
	res = f.exec(second, nil, false)
	if second.Status() != wager.StatusRejected || second.FailureCode() != wager.FailureInsufficientFunds || res.Entry != nil {
		t.Fatalf("second bet: %s %s", second.Status(), second.FailureCode())
	}
	if got := eventTypes(second.PullEvents()); !equal(got, []string{events.TypeWagerTransactionRejected}) {
		t.Fatalf("events %v", got)
	}
	if f.wallet.Balance().Amount() != "20.00" {
		t.Fatal("rejected bet changed the balance")
	}
}

func TestWinCreditsWithOptionalReference(t *testing.T) {
	f := newFixture(t, "10.00")
	win := f.tx(wager.KindWin, "w1", "5.00", "")
	if res := f.exec(win, nil, false); res.Entry == nil || win.Status() != wager.StatusProcessed || f.wallet.Balance().Amount() != "15.00" {
		t.Fatalf("win: %s %s", win.Status(), f.wallet.Balance())
	}
	bet := f.tx(wager.KindBet, "b1", "2.00", "")
	f.exec(bet, nil, false)
	refWin := f.tx(wager.KindWin, "w2", "50.00", "b1")
	f.exec(refWin, bet, false)
	if refWin.Status() != wager.StatusProcessed || refWin.ReferenceTransactionID() == nil || *refWin.ReferenceTransactionID() != bet.ID() {
		t.Fatalf("referenced win: %s", refWin.Status())
	}
	// A WIN cannot reference another WIN.
	bad := f.tx(wager.KindWin, "w3", "1.00", "w1")
	f.exec(bad, win, false)
	if bad.FailureCode() != wager.FailureReferenceKindInvalid {
		t.Fatalf("win->win: %s", bad.FailureCode())
	}
}

func TestLossHasNoMovement(t *testing.T) {
	f := newFixture(t, "10.00")
	loss := f.tx(wager.KindLoss, "l1", "0.00", "")
	res := f.exec(loss, nil, false)
	if res.Entry != nil || loss.Status() != wager.StatusProcessed || f.wallet.Version() != 1 || f.wallet.Changed() {
		t.Fatalf("loss: entry=%v status=%s version=%d", res.Entry, loss.Status(), f.wallet.Version())
	}
	if got := eventTypes(loss.PullEvents()); !equal(got, []string{events.TypeWagerTransactionProcessed}) {
		t.Fatalf("events %v (LOSS must not emit WalletBalanceChanged)", got)
	}
}

func TestRefundAndRollbackRules(t *testing.T) {
	f := newFixture(t, "100.00")
	bet := f.tx(wager.KindBet, "b1", "30.00", "")
	f.exec(bet, nil, false)

	refund := f.tx(wager.KindRefund, "r1", "30.00", "b1")
	if res := f.exec(refund, bet, false); res.Entry == nil || res.Entry.Direction() != wallet.Credit || f.wallet.Balance().Amount() != "100.00" {
		t.Fatalf("refund: %s", refund.Status())
	}
	// Second reversal of the same BET (refund or rollback) is rejected.
	again := f.tx(wager.KindRefund, "r2", "30.00", "b1")
	f.exec(again, bet, true)
	if again.FailureCode() != wager.FailureReferenceAlreadyReversed {
		t.Fatalf("double refund: %s", again.FailureCode())
	}
	rb := f.tx(wager.KindRollback, "rb1", "30.00", "b1")
	f.exec(rb, bet, true)
	if rb.FailureCode() != wager.FailureReferenceAlreadyReversed {
		t.Fatalf("rollback after refund: %s", rb.FailureCode())
	}
	// ROLLBACK of the REFUND debits it back.
	rbRefund := f.tx(wager.KindRollback, "rb2", "30.00", "r1")
	if res := f.exec(rbRefund, refund, false); res.Entry == nil || res.Entry.Direction() != wallet.Debit || f.wallet.Balance().Amount() != "70.00" {
		t.Fatalf("rollback refund: %s %s", rbRefund.Status(), f.wallet.Balance())
	}
	// Partial amounts are rejected.
	bet2 := f.tx(wager.KindBet, "b2", "10.00", "")
	f.exec(bet2, nil, false)
	partial := f.tx(wager.KindRefund, "r3", "5.00", "b2")
	f.exec(partial, bet2, false)
	if partial.FailureCode() != wager.FailureReferenceAmountMismatch {
		t.Fatalf("partial: %s", partial.FailureCode())
	}
	// REFUND must reference a BET.
	win := f.tx(wager.KindWin, "w1", "10.00", "")
	f.exec(win, nil, false)
	refundWin := f.tx(wager.KindRefund, "r4", "10.00", "w1")
	f.exec(refundWin, win, false)
	if refundWin.FailureCode() != wager.FailureReferenceKindInvalid {
		t.Fatalf("refund of win: %s", refundWin.FailureCode())
	}
}

func TestRollbackOfWinWithoutBalanceIsRejectedWithDistinctCode(t *testing.T) {
	f := newFixture(t, "0.00")
	win := f.tx(wager.KindWin, "w1", "50.00", "")
	f.exec(win, nil, false)
	bet := f.tx(wager.KindBet, "b1", "40.00", "")
	f.exec(bet, nil, false)
	rb := f.tx(wager.KindRollback, "rb1", "50.00", "w1")
	f.exec(rb, win, false)
	if rb.Status() != wager.StatusRejected || rb.FailureCode() != wager.FailureReversalInsufficientFunds {
		t.Fatalf("rollback: %s %s", rb.Status(), rb.FailureCode())
	}
	if wager.FailureReversalInsufficientFunds == wager.FailureInsufficientFunds {
		t.Fatal("codes must differ")
	}
	if f.wallet.Balance().Amount() != "10.00" {
		t.Fatalf("balance %s", f.wallet.Balance())
	}
}

func TestReferenceMismatchAndUnprocessedReference(t *testing.T) {
	f := newFixture(t, "100.00")
	bet := f.tx(wager.KindBet, "b1", "10.00", "")
	f.exec(bet, nil, false)

	other := f.req(wager.KindRefund, "r1", "10.00", "b1")
	other.RoundID = "round-2"
	refund, _ := wager.NewExternal(uuid.New(), other, events.Meta{}, now)
	f.exec(refund, bet, false)
	if refund.FailureCode() != wager.FailureReferenceMismatch {
		t.Fatalf("round mismatch: %s", refund.FailureCode())
	}

	rejectedBet := f.tx(wager.KindBet, "b2", "1000.00", "")
	f.exec(rejectedBet, nil, false)
	refund2 := f.tx(wager.KindRefund, "r2", "1000.00", "b2")
	f.exec(refund2, rejectedBet, false)
	if refund2.FailureCode() != wager.FailureReferenceNotProcessed {
		t.Fatalf("refund of rejected bet: %s", refund2.FailureCode())
	}
}

func TestPendingReferenceLifecycle(t *testing.T) {
	f := newFixture(t, "100.00")
	refund := f.tx(wager.KindRefund, "r1", "10.00", "b1")
	f.exec(refund, nil, false)
	if refund.Status() != wager.StatusPendingReference || refund.NextAttemptAt() == nil {
		t.Fatalf("status %s", refund.Status())
	}
	if got := eventTypes(refund.PullEvents()); !equal(got, []string{events.TypeWagerTransactionPendingReference}) {
		t.Fatalf("events %v", got)
	}
	// Retries do not emit the pending event again.
	f.exec(refund, nil, false)
	if refund.ReferenceAttempts() != 1 || len(refund.PullEvents()) != 0 {
		t.Fatalf("attempts %d", refund.ReferenceAttempts())
	}
	// The reference arrives.
	bet := f.tx(wager.KindBet, "b1", "10.00", "")
	f.exec(bet, nil, false)
	f.exec(refund, bet, false)
	if refund.Status() != wager.StatusProcessed || f.wallet.Balance().Amount() != "100.00" {
		t.Fatalf("resolved: %s %s", refund.Status(), f.wallet.Balance())
	}
}

func TestPendingReferenceExpires(t *testing.T) {
	f := newFixture(t, "100.00")
	rb := f.tx(wager.KindRollback, "rb1", "10.00", "missing")
	for i := 0; i < 3; i++ {
		f.exec(rb, nil, false)
	}
	if rb.Status() != wager.StatusPendingReference {
		t.Fatalf("status %s after 3 attempts", rb.Status())
	}
	f.exec(rb, nil, false)
	if rb.Status() != wager.StatusRejected || rb.FailureCode() != wager.FailureReferenceNotFound {
		t.Fatalf("expired: %s %s", rb.Status(), rb.FailureCode())
	}
	evs := eventTypes(rb.PullEvents())
	if evs[len(evs)-1] != events.TypeWagerTransactionRejected {
		t.Fatalf("events %v", evs)
	}
}

func TestWalletMismatches(t *testing.T) {
	f := newFixture(t, "100.00")
	r := f.req(wager.KindBet, "b1", "1.00", "")
	r.PlayerID = uuid.New()
	bet, _ := wager.NewExternal(uuid.New(), r, events.Meta{}, now)
	f.exec(bet, nil, false)
	if bet.FailureCode() != wager.FailureWalletPlayerMismatch || bet.ResultBalance() != nil {
		t.Fatalf("player mismatch: %s", bet.FailureCode())
	}
	r = f.req(wager.KindBet, "b2", "1.00", "")
	r.Money, _ = money.Parse("1.00", money.MustCurrency("USD"))
	bet2, _ := wager.NewExternal(uuid.New(), r, events.Meta{}, now)
	f.exec(bet2, nil, false)
	if bet2.FailureCode() != wager.FailureWalletCurrencyMismatch {
		t.Fatalf("currency mismatch: %s", bet2.FailureCode())
	}
}

func TestTerminalStatesAreFinal(t *testing.T) {
	f := newFixture(t, "100.00")
	bet := f.tx(wager.KindBet, "b1", "1.00", "")
	f.exec(bet, nil, false)
	if _, err := bet.Execute(wager.ExecuteParams{Wallet: f.wallet, LedgerEntryID: uuid.New(), Schedule: schedule{3}, Now: now}); !errors.Is(err, wager.ErrInvalidTransition) {
		t.Fatalf("re-execute: %v", err)
	}
	if err := bet.MarkRejected(wager.FailureInsufficientFunds, nil, now); !errors.Is(err, wager.ErrInvalidTransition) {
		t.Fatalf("reject processed: %v", err)
	}
	if err := bet.MarkFailed(wager.FailurePermanentProcessingError, now); !errors.Is(err, wager.ErrInvalidTransition) {
		t.Fatalf("fail processed: %v", err)
	}
	pending := f.tx(wager.KindBet, "b2", "1.00", "")
	if err := pending.MarkPendingReference(now, now); !errors.Is(err, wager.ErrInvalidTransition) {
		t.Fatalf("pending reference without reference: %v", err)
	}
	if err := pending.MarkFailed(wager.FailurePermanentProcessingError, now); err != nil || pending.Status() != wager.StatusFailed {
		t.Fatalf("fail pending: %v", err)
	}
}

func TestRehydrateDoesNotEmitEvents(t *testing.T) {
	f := newFixture(t, "100.00")
	bet := f.tx(wager.KindBet, "b1", "1.00", "")
	f.exec(bet, nil, false)
	back, err := wager.Rehydrate(bet.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	if len(back.PullEvents()) != 0 || back.Status() != wager.StatusProcessed {
		t.Fatal("rehydration must not record events or transitions")
	}
	s := bet.Snapshot()
	s.Status = "BOGUS"
	if _, err := wager.Rehydrate(s); err == nil {
		t.Error("bogus status accepted")
	}
	s = bet.Snapshot()
	s.Status, s.FailureCode = wager.StatusRejected, ""
	if _, err := wager.Rehydrate(s); err == nil {
		t.Error("rejected without code accepted")
	}
}

func TestOpeningTransaction(t *testing.T) {
	player, walletID, txID := uuid.New(), uuid.New(), uuid.New()
	w, entry, err := wallet.Open(wallet.OpenParams{WalletID: walletID, PlayerID: player, InitialBalance: amt(t, "1000.00"),
		OpeningTransactionID: txID, LedgerEntryID: uuid.New(), Now: now})
	if err != nil {
		t.Fatal(err)
	}
	op, err := wager.NewOpening(txID, w.ID(), player, amt(t, "1000.00"), events.Meta{CorrelationID: "c"}, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := op.CompleteOpening(*entry, now); err != nil {
		t.Fatal(err)
	}
	s := op.Snapshot()
	if s.Origin != wager.OriginInternal || s.Kind != wager.KindOpening || s.Status != wager.StatusProcessed ||
		s.ProviderID != "" || s.ExternalTransactionID != "" || s.IdempotencyKey != "" || s.PayloadHash != "" || s.RoundID != "" || s.GameID != "" {
		t.Fatalf("opening snapshot: %+v", s)
	}
	evs := op.PullEvents()
	if got := eventTypes(evs); !equal(got, []string{events.TypeWagerTransactionProcessed, events.TypeWalletBalanceChanged}) {
		t.Fatalf("events %v", got)
	}
	bc := evs[1].(events.WalletBalanceChanged)
	if bc.Data.WalletVersion != 1 || bc.Data.BalanceBefore.Amount() != "0.00" || bc.Data.BalanceAfter.Amount() != "1000.00" {
		t.Fatalf("balance changed: %+v", bc.Data)
	}
	payload, _ := evs[0].Payload()
	if got := string(payload); !contains(got, `"origin":"INTERNAL"`) || contains(got, `"providerId"`) {
		t.Fatalf("opening event payload must omit external metadata: %s", got)
	}
	if _, err := wager.NewOpening(uuid.New(), w.ID(), player, amt(t, "0.00"), events.Meta{}, now); err == nil {
		t.Error("zero OPENING accepted")
	}
	if _, err := op.Execute(wager.ExecuteParams{Wallet: w, Now: now}); err == nil {
		t.Error("OPENING must not be executable as an external operation")
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
