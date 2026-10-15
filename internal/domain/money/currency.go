package money

import "github.com/julioCAlmeida/backend-challenge-go/internal/domain/derr"

// Currency is an ISO 4217 alphabetic code. The zero value is invalid.
type Currency struct {
	code string
}

// supportedCurrencies lists ISO 4217 codes whose minor unit is exactly two digits.
// The contract uses a fixed scale of two decimal places, so currencies with a
// different exponent (JPY, KWD, ...) are rejected instead of silently rescaled.
var supportedCurrencies = map[string]struct{}{
	"BRL": {}, "USD": {}, "EUR": {}, "GBP": {}, "ARS": {}, "CAD": {}, "AUD": {},
	"CHF": {}, "MXN": {}, "COP": {}, "PEN": {}, "UYU": {}, "CNY": {}, "INR": {},
}

var (
	ErrInvalidCurrency       = derr.New(derr.KindValidation, "INVALID_CURRENCY", "currency must be a supported ISO 4217 code with two decimal places")
	ErrUninitializedCurrency = derr.New(derr.KindValidation, "UNINITIALIZED_CURRENCY", "currency is not initialized")
)

// BRL is the primary currency of the challenge scenarios.
var BRL = Currency{code: "BRL"}

// ParseCurrency validates an ISO 4217 code. The code must already be uppercase:
// no normalization is applied, so the idempotency hash sees exactly what was sent.
func ParseCurrency(code string) (Currency, error) {
	if _, ok := supportedCurrencies[code]; !ok {
		return Currency{}, derr.Wrapf(ErrInvalidCurrency, "unsupported currency %q", code)
	}
	return Currency{code: code}, nil
}

// MustCurrency is intended for tests and constants only.
func MustCurrency(code string) Currency {
	c, err := ParseCurrency(code)
	if err != nil {
		panic(err)
	}
	return c
}

// Code returns the ISO 4217 code.
func (c Currency) Code() string { return c.code }

// IsZero reports whether the currency was never initialized.
func (c Currency) IsZero() bool { return c.code == "" }

func (c Currency) String() string { return c.code }

// Validate rejects the zero value.
func (c Currency) Validate() error {
	if c.code == "" {
		return ErrUninitializedCurrency
	}
	return nil
}
