package money_test

import (
	"encoding/json"
	"errors"
	"math"
	"testing"

	"github.com/julioCAlmeida/backend-challenge-go/internal/domain/money"
)

var brl = money.BRL

func mustParse(t *testing.T, s string) money.Money {
	t.Helper()
	m, err := money.Parse(s, brl)
	if err != nil {
		t.Fatalf("Parse(%q): %v", s, err)
	}
	return m
}

func TestParseValid(t *testing.T) {
	cases := map[string]int64{
		"0.00": 0, "0.01": 1, "25.00": 2500, "975.00": 97500, "-3.10": -310, "1000.00": 100000,
		"92233720368547758.07": math.MaxInt64,
	}
	for in, want := range cases {
		m := mustParse(t, in)
		if m.MinorUnits() != want {
			t.Errorf("Parse(%q) = %d, want %d", in, m.MinorUnits(), want)
		}
		if m.Amount() != in {
			t.Errorf("round trip %q -> %q", in, m.Amount())
		}
	}
}

func TestParseRejectsInvalidInputsWithoutRounding(t *testing.T) {
	invalid := []string{
		"", " ", "NaN", "nan", "Infinity", "-Infinity", "+Inf", "1e3", "1E3", "2.5e1", "25", "25.0", "25.000",
		"25.001", ".50", "25.", "+25.00", "025.00", "00.00", "-0.00", "1,000.00", "1 000.00", "25.00 ", " 25.00",
		"0x10.00", "--1.00", "1.-1", "١٢.٠٠", "25,00",
	}
	for _, in := range invalid {
		if _, err := money.Parse(in, brl); err == nil {
			t.Errorf("Parse(%q) accepted an invalid amount", in)
		}
	}
}

func TestParseOverflow(t *testing.T) {
	for _, in := range []string{"92233720368547758.08", "99999999999999999999.99", "-92233720368547758.08"} {
		_, err := money.Parse(in, brl)
		if !errors.Is(err, money.ErrOverflow) {
			t.Errorf("Parse(%q) err = %v, want ErrOverflow", in, err)
		}
	}
}

func TestParseNonNegativeRejectsNegative(t *testing.T) {
	_, err := money.ParseNonNegative("-1.00", "BRL")
	if !errors.Is(err, money.ErrNegativeAmount) {
		t.Fatalf("err = %v, want ErrNegativeAmount", err)
	}
	if _, err := money.ParseNonNegative("0.00", "BRL"); err != nil {
		t.Fatalf("zero must be accepted: %v", err)
	}
}

func TestCurrencyValidation(t *testing.T) {
	for _, c := range []string{"", "brl", "BR", "BRLL", "XXX", "JPY", "KWD"} {
		if _, err := money.ParseCurrency(c); err == nil {
			t.Errorf("ParseCurrency(%q) accepted", c)
		}
	}
	if _, err := money.ParseNonNegative("1.00", "usd"); !errors.Is(err, money.ErrInvalidCurrency) {
		t.Errorf("lowercase currency must be rejected, got %v", err)
	}
}

func TestZeroValueIsRejected(t *testing.T) {
	var m money.Money
	if m.IsValid() {
		t.Fatal("zero value must be invalid")
	}
	if _, err := m.Add(mustParse(t, "1.00")); !errors.Is(err, money.ErrUninitialized) {
		t.Errorf("Add on zero value: %v", err)
	}
	if _, err := m.Negate(); !errors.Is(err, money.ErrUninitialized) {
		t.Errorf("Negate on zero value: %v", err)
	}
	if _, err := json.Marshal(m); err == nil {
		t.Error("marshal of zero value must fail")
	}
	if _, err := money.Zero(money.Currency{}); err == nil {
		t.Error("Zero with uninitialized currency must fail")
	}
	if _, err := money.Parse("1.00", money.Currency{}); err == nil {
		t.Error("Parse with uninitialized currency must fail")
	}
}

func TestArithmetic(t *testing.T) {
	a, b := mustParse(t, "100.00"), mustParse(t, "80.00")
	sum, err := a.Add(b)
	if err != nil || sum.Amount() != "180.00" {
		t.Fatalf("Add = %v %v", sum, err)
	}
	diff, err := b.Sub(a)
	if err != nil || diff.Amount() != "-20.00" || !diff.IsNegative() {
		t.Fatalf("Sub = %v %v", diff, err)
	}
	neg, err := a.Negate()
	if err != nil || neg.Amount() != "-100.00" {
		t.Fatalf("Negate = %v %v", neg, err)
	}
	if c, _ := a.Cmp(b); c != 1 {
		t.Errorf("Cmp = %d", c)
	}
	if c, _ := b.Cmp(a); c != -1 {
		t.Errorf("Cmp = %d", c)
	}
	if c, _ := a.Cmp(a); c != 0 {
		t.Errorf("Cmp = %d", c)
	}
	zero, _ := money.Zero(brl)
	if !zero.IsZero() || zero.Amount() != "0.00" {
		t.Errorf("Zero = %v", zero)
	}
	// immutability: operands unchanged
	if a.Amount() != "100.00" || b.Amount() != "80.00" {
		t.Error("operands were mutated")
	}
}

func TestArithmeticOverflow(t *testing.T) {
	max, _ := money.FromMinor(math.MaxInt64, brl)
	minVal, _ := money.FromMinor(-math.MaxInt64, brl)
	one := mustParse(t, "0.01")
	if _, err := max.Add(one); !errors.Is(err, money.ErrOverflow) {
		t.Errorf("max+1: %v", err)
	}
	if _, err := minVal.Sub(one); !errors.Is(err, money.ErrOverflow) {
		t.Errorf("min-1: %v", err)
	}
	if _, err := money.FromMinor(math.MinInt64, brl); !errors.Is(err, money.ErrOverflow) {
		t.Errorf("FromMinor(MinInt64): %v", err)
	}
	if n, err := minVal.Negate(); err != nil || n.MinorUnits() != math.MaxInt64 {
		t.Errorf("Negate(min) = %v, %v", n, err)
	}
}

func TestCurrencyMismatch(t *testing.T) {
	usd := money.MustCurrency("USD")
	a := mustParse(t, "1.00")
	b, _ := money.Parse("1.00", usd)
	if _, err := a.Add(b); !errors.Is(err, money.ErrCurrencyMismatch) {
		t.Errorf("Add: %v", err)
	}
	if _, err := a.Sub(b); !errors.Is(err, money.ErrCurrencyMismatch) {
		t.Errorf("Sub: %v", err)
	}
	if _, err := a.Cmp(b); !errors.Is(err, money.ErrCurrencyMismatch) {
		t.Errorf("Cmp: %v", err)
	}
	if a.Equal(b) {
		t.Error("different currencies must not be equal")
	}
}

func TestJSON(t *testing.T) {
	m := mustParse(t, "25.00")
	b, err := json.Marshal(m)
	if err != nil || string(b) != `{"amount":"25.00","currency":"BRL"}` {
		t.Fatalf("Marshal = %s %v", b, err)
	}
	var back money.Money
	if err := json.Unmarshal(b, &back); err != nil || !back.Equal(m) {
		t.Fatalf("Unmarshal = %v %v", back, err)
	}
	for _, in := range []string{
		`{"amount":25.00,"currency":"BRL"}`, `{"amount":"25","currency":"BRL"}`, `{"amount":"25.00"}`,
		`{"amount":"25.00","currency":"BRL","x":1}`, `{"amount":null,"currency":"BRL"}`,
	} {
		var x money.Money
		if err := json.Unmarshal([]byte(in), &x); err == nil {
			t.Errorf("Unmarshal(%s) accepted", in)
		}
	}
}

func FuzzParseRoundTrip(f *testing.F) {
	for _, s := range []string{"0.00", "1.23", "-5.00", "1e3", "NaN", "92233720368547758.07"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		m, err := money.Parse(s, brl)
		if err != nil {
			return
		}
		if m.Amount() != s {
			t.Fatalf("accepted %q but renders %q", s, m.Amount())
		}
	})
}
