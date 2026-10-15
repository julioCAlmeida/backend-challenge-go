package wallet_test

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/julioCAlmeida/backend-challenge-go/internal/domain/money"
	"github.com/julioCAlmeida/backend-challenge-go/internal/domain/wallet"
)

var now = time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)

func m(t *testing.T, s string) money.Money {
	t.Helper()
	v, err := money.Parse(s, money.BRL)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func open(t *testing.T, initial string) (*wallet.Wallet, *wallet.LedgerEntry) {
	t.Helper()
	w, e, err := wallet.Open(wallet.OpenParams{
		WalletID: uuid.New(), PlayerID: uuid.New(), InitialBalance: m(t, initial),
		OpeningTransactionID: uuid.New(), LedgerEntryID: uuid.New(), Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	return w, e
}

func TestOpenWithPositiveBalanceCreatesOpeningCredit(t *testing.T) {
	w, e := open(t, "100.00")
	if w.Version() != 1 || w.Balance().Amount() != "100.00" {
		t.Fatalf("wallet = v%d %s", w.Version(), w.Balance())
	}
	if e == nil || e.Direction() != wallet.Credit || e.BalanceBefore().Amount() != "0.00" || e.BalanceAfter().Amount() != "100.00" || e.WalletVersion() != 1 {
		t.Fatalf("entry = %+v", e)
	}
}

func TestOpenWithZeroBalanceHasNoEntry(t *testing.T) {
	w, e := open(t, "0.00")
	if e != nil || w.Version() != 1 || !w.Balance().IsZero() {
		t.Fatalf("zero opening: entry=%v version=%d", e, w.Version())
	}
}

func TestOpenRejectsInvalidInput(t *testing.T) {
	neg, _ := money.Parse("-1.00", money.BRL)
	cases := []wallet.OpenParams{
		{WalletID: uuid.Nil, PlayerID: uuid.New(), InitialBalance: m(t, "1.00"), Now: now},
		{WalletID: uuid.New(), PlayerID: uuid.Nil, InitialBalance: m(t, "1.00"), Now: now},
		{WalletID: uuid.New(), PlayerID: uuid.New(), InitialBalance: money.Money{}, Now: now},
		{WalletID: uuid.New(), PlayerID: uuid.New(), InitialBalance: neg, Now: now},
		{WalletID: uuid.New(), PlayerID: uuid.New(), InitialBalance: m(t, "1.00")},
	}
	for i, p := range cases {
		if _, _, err := wallet.Open(p); err == nil {
			t.Errorf("case %d accepted", i)
		}
	}
}

func TestDebitPreservesNonNegativeBalance(t *testing.T) {
	w, _ := open(t, "100.00")
	e, err := w.Debit(m(t, "80.00"), uuid.New(), uuid.New(), now)
	if err != nil {
		t.Fatal(err)
	}
	if w.Balance().Amount() != "20.00" || w.Version() != 2 || e.BalanceAfter().Amount() != "20.00" || e.WalletVersion() != 2 {
		t.Fatalf("after debit: %s v%d", w.Balance(), w.Version())
	}
	_, err = w.Debit(m(t, "80.00"), uuid.New(), uuid.New(), now)
	if !errors.Is(err, wallet.ErrInsufficientFunds) {
		t.Fatalf("err = %v, want insufficient funds", err)
	}
	if w.Balance().Amount() != "20.00" || w.Version() != 2 {
		t.Fatal("a rejected debit must not change the wallet")
	}
	if _, err := w.Debit(m(t, "20.00"), uuid.New(), uuid.New(), now); err != nil {
		t.Fatalf("debit to exactly zero: %v", err)
	}
}

func TestCreditIncrementsVersion(t *testing.T) {
	w, _ := open(t, "0.00")
	if _, err := w.Credit(m(t, "5.00"), uuid.New(), uuid.New(), now); err != nil {
		t.Fatal(err)
	}
	if w.Version() != 2 || w.Balance().Amount() != "5.00" {
		t.Fatalf("%s v%d", w.Balance(), w.Version())
	}
}

func TestMovementRules(t *testing.T) {
	w, _ := open(t, "10.00")
	usd, _ := money.Parse("1.00", money.MustCurrency("USD"))
	if _, err := w.Credit(usd, uuid.New(), uuid.New(), now); !errors.Is(err, wallet.ErrCurrencyMismatch) {
		t.Errorf("currency mismatch: %v", err)
	}
	if _, err := w.Credit(m(t, "0.00"), uuid.New(), uuid.New(), now); !errors.Is(err, wallet.ErrInvalidMovement) {
		t.Errorf("zero movement: %v", err)
	}
	if _, err := w.Debit(money.Money{}, uuid.New(), uuid.New(), now); err == nil {
		t.Error("uninitialized money accepted")
	}
	if w.Version() != 1 {
		t.Error("failed movements changed the version")
	}
}

func TestRehydrateDoesNotApplyMovements(t *testing.T) {
	id, player := uuid.New(), uuid.New()
	w, err := wallet.Rehydrate(wallet.Snapshot{ID: id, PlayerID: player, Balance: m(t, "42.00"), Version: 7, CreatedAt: now, UpdatedAt: now})
	if err != nil {
		t.Fatal(err)
	}
	if w.Version() != 7 || w.PersistedVersion() != 7 || w.Balance().Amount() != "42.00" || w.Changed() || w.IsNew() {
		t.Fatalf("rehydrated wallet: %+v", w.Snapshot())
	}
	neg, _ := money.Parse("-1.00", money.BRL)
	for _, s := range []wallet.Snapshot{
		{ID: id, PlayerID: player, Balance: neg, Version: 1},
		{ID: id, PlayerID: player, Balance: m(t, "1.00"), Version: 0},
		{ID: uuid.Nil, PlayerID: player, Balance: m(t, "1.00"), Version: 1},
		{ID: id, PlayerID: player, Balance: money.Money{}, Version: 1},
	} {
		if _, err := wallet.Rehydrate(s); err == nil {
			t.Errorf("invalid snapshot accepted: %+v", s)
		}
	}
}

func TestLedgerEntryValidatesArithmetic(t *testing.T) {
	base := wallet.LedgerEntrySnapshot{
		ID: uuid.New(), WalletID: uuid.New(), TransactionID: uuid.New(), Direction: wallet.Debit,
		Amount: m(t, "25.00"), BalanceBefore: m(t, "1000.00"), BalanceAfter: m(t, "975.00"), WalletVersion: 2, CreatedAt: now,
	}
	if _, err := wallet.NewLedgerEntry(base); err != nil {
		t.Fatalf("valid entry rejected: %v", err)
	}
	bad := base
	bad.BalanceAfter = m(t, "976.00")
	if _, err := wallet.NewLedgerEntry(bad); !errors.Is(err, wallet.ErrInvalidLedgerEntry) {
		t.Errorf("wrong arithmetic accepted: %v", err)
	}
	bad = base
	bad.Direction = wallet.Credit
	if _, err := wallet.NewLedgerEntry(bad); err == nil {
		t.Error("wrong direction accepted")
	}
	bad = base
	bad.Amount = m(t, "0.00")
	if _, err := wallet.NewLedgerEntry(bad); err == nil {
		t.Error("zero amount accepted")
	}
	bad = base
	bad.Direction = "SIDEWAYS"
	if _, err := wallet.NewLedgerEntry(bad); err == nil {
		t.Error("unknown direction accepted")
	}
}
