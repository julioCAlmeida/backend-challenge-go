// Package money implements the Money value object.
//
// Representation: a signed int64 count of minor units (cents) plus an ISO 4217
// currency. The scale is fixed at two decimal places, so the representable range
// is [-92233720368547758.07, 92233720368547758.07]. Every arithmetic operation is
// overflow-checked; no float type is ever involved in parsing, arithmetic,
// serialization or persistence.
package money

import (
	"bytes"
	"encoding/json"
	"math"
	"strconv"

	"github.com/julioCAlmeida/backend-challenge-go/internal/domain/derr"
)

// Scale is the fixed number of decimal places of every amount.
const Scale = 2

var (
	ErrInvalidAmount    = derr.New(derr.KindValidation, "INVALID_AMOUNT", "amount must be a decimal string with exactly two decimal places")
	ErrNegativeAmount   = derr.New(derr.KindValidation, "NEGATIVE_AMOUNT", "amount must not be negative")
	ErrOverflow         = derr.New(derr.KindValidation, "AMOUNT_OVERFLOW", "amount exceeds the representable range")
	ErrCurrencyMismatch = derr.New(derr.KindValidation, "CURRENCY_MISMATCH", "money operations require the same currency")
	ErrUninitialized    = derr.New(derr.KindValidation, "UNINITIALIZED_MONEY", "money value is not initialized")
)

// Money is an immutable amount in a currency. The zero value is invalid: every
// operation on it returns ErrUninitialized.
type Money struct {
	minor    int64
	currency Currency
}

// Zero returns zero in the given currency.
func Zero(c Currency) (Money, error) {
	if err := c.Validate(); err != nil {
		return Money{}, err
	}
	return Money{currency: c}, nil
}

// FromMinor rebuilds a Money from persisted minor units. Used for rehydration.
func FromMinor(minor int64, c Currency) (Money, error) {
	if err := c.Validate(); err != nil {
		return Money{}, err
	}
	if minor == math.MinInt64 {
		// MinInt64 cannot be negated; keep the range symmetric.
		return Money{}, ErrOverflow
	}
	return Money{minor: minor, currency: c}, nil
}

// Parse parses a signed canonical decimal string such as "25.00" or "-3.10".
//
// Accepted grammar: -?(0|[1-9][0-9]*)\.[0-9]{2}
//
// Everything else is rejected without rounding: empty strings, NaN, Infinity,
// scientific notation, a '+' sign, missing or extra decimal places, leading
// zeros, whitespace, thousands separators and "-0.00". Because only the canonical
// form is accepted, no normalization happens before the idempotency hash.
func Parse(s string, c Currency) (Money, error) {
	if err := c.Validate(); err != nil {
		return Money{}, err
	}
	neg := false
	body := s
	if len(body) > 0 && body[0] == '-' {
		neg = true
		body = body[1:]
	}
	minor, err := parseUnsigned(body)
	if err != nil {
		return Money{}, derr.Wrapf(err.(*derr.Error), "invalid amount %q", s)
	}
	if neg {
		if minor == 0 {
			return Money{}, derr.Wrapf(ErrInvalidAmount, "negative zero %q is not canonical", s)
		}
		minor = -minor
	}
	return Money{minor: minor, currency: c}, nil
}

// ParseNonNegative parses an external financial input: like Parse, but negative
// values are rejected.
func ParseNonNegative(amount, currency string) (Money, error) {
	c, err := ParseCurrency(currency)
	if err != nil {
		return Money{}, err
	}
	if len(amount) > 0 && amount[0] == '-' {
		return Money{}, derr.Wrapf(ErrNegativeAmount, "negative amount %q", amount)
	}
	return Parse(amount, c)
}

func parseUnsigned(s string) (int64, error) {
	dot := len(s) - (Scale + 1)
	if dot < 1 || s[dot] != '.' {
		return 0, ErrInvalidAmount
	}
	intPart, frac := s[:dot], s[dot+1:]
	if len(intPart) > 1 && intPart[0] == '0' {
		return 0, ErrInvalidAmount
	}
	var v int64
	for _, part := range []string{intPart, frac} {
		for i := 0; i < len(part); i++ {
			ch := part[i]
			if ch < '0' || ch > '9' {
				return 0, ErrInvalidAmount
			}
			d := int64(ch - '0')
			if v > (math.MaxInt64-d)/10 {
				return 0, ErrOverflow
			}
			v = v*10 + d
		}
	}
	return v, nil
}

// MinorUnits returns the amount in minor units (cents).
func (m Money) MinorUnits() int64 { return m.minor }

// Currency returns the currency.
func (m Money) Currency() Currency { return m.currency }

// IsValid reports whether the value was properly constructed.
func (m Money) IsValid() bool { return !m.currency.IsZero() }

// Validate rejects the zero value.
func (m Money) Validate() error {
	if !m.IsValid() {
		return ErrUninitialized
	}
	return nil
}

func (m Money) IsZero() bool     { return m.minor == 0 }
func (m Money) IsNegative() bool { return m.minor < 0 }
func (m Money) IsPositive() bool { return m.minor > 0 }

func (m Money) compatible(o Money) error {
	if !m.IsValid() || !o.IsValid() {
		return ErrUninitialized
	}
	if m.currency != o.currency {
		return derr.Wrapf(ErrCurrencyMismatch, "%s vs %s", m.currency, o.currency)
	}
	return nil
}

// Add returns m + o.
func (m Money) Add(o Money) (Money, error) {
	if err := m.compatible(o); err != nil {
		return Money{}, err
	}
	if (o.minor > 0 && m.minor > math.MaxInt64-o.minor) ||
		(o.minor < 0 && m.minor < -math.MaxInt64-o.minor) {
		return Money{}, ErrOverflow
	}
	return Money{minor: m.minor + o.minor, currency: m.currency}, nil
}

// Sub returns m - o.
func (m Money) Sub(o Money) (Money, error) {
	if err := m.compatible(o); err != nil {
		return Money{}, err
	}
	neg, err := o.Negate()
	if err != nil {
		return Money{}, err
	}
	return m.Add(neg)
}

// Negate returns -m.
func (m Money) Negate() (Money, error) {
	if err := m.Validate(); err != nil {
		return Money{}, err
	}
	if m.minor == math.MinInt64 {
		return Money{}, ErrOverflow
	}
	return Money{minor: -m.minor, currency: m.currency}, nil
}

// Cmp returns -1, 0 or 1. Currencies must match.
func (m Money) Cmp(o Money) (int, error) {
	if err := m.compatible(o); err != nil {
		return 0, err
	}
	switch {
	case m.minor < o.minor:
		return -1, nil
	case m.minor > o.minor:
		return 1, nil
	}
	return 0, nil
}

// Equal reports whether both values have the same amount and currency.
func (m Money) Equal(o Money) bool {
	return m.IsValid() && m.currency == o.currency && m.minor == o.minor
}

// Amount renders the canonical decimal string, e.g. "975.00" or "-3.10".
func (m Money) Amount() string {
	u := uint64(m.minor)
	sign := ""
	if m.minor < 0 {
		sign = "-"
		u = uint64(-m.minor)
	}
	digits := strconv.FormatUint(u, 10)
	for len(digits) <= Scale {
		digits = "0" + digits
	}
	cut := len(digits) - Scale
	return sign + digits[:cut] + "." + digits[cut:]
}

func (m Money) String() string { return m.Amount() + " " + m.currency.Code() }

type wire struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

// MarshalJSON renders {"amount":"25.00","currency":"BRL"}.
func (m Money) MarshalJSON() ([]byte, error) {
	if err := m.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(wire{Amount: m.Amount(), Currency: m.currency.Code()})
}

// UnmarshalJSON accepts only the string form for amount; JSON numbers are
// rejected so a value never transits through float64.
func (m *Money) UnmarshalJSON(b []byte) error {
	var raw struct {
		Amount   json.RawMessage `json:"amount"`
		Currency string          `json:"currency"`
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&raw); err != nil {
		return derr.Wrapf(ErrInvalidAmount, "malformed money: %v", err)
	}
	if len(raw.Amount) == 0 || raw.Amount[0] != '"' {
		return derr.Wrapf(ErrInvalidAmount, "amount must be a JSON string")
	}
	var amount string
	if err := json.Unmarshal(raw.Amount, &amount); err != nil {
		return derr.Wrapf(ErrInvalidAmount, "malformed amount")
	}
	c, err := ParseCurrency(raw.Currency)
	if err != nil {
		return err
	}
	v, err := Parse(amount, c)
	if err != nil {
		return err
	}
	*m = v
	return nil
}
