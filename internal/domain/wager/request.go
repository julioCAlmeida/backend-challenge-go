package wager

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"unicode"

	"github.com/google/uuid"

	"github.com/julioCAlmeida/backend-challenge-go/internal/domain/derr"
	"github.com/julioCAlmeida/backend-challenge-go/internal/domain/money"
)

const (
	maxIdentifierLen     = 128
	maxIdempotencyKeyLen = 255
)

// Request is a validated external operation, identical for HTTP and SQS.
type Request struct {
	ProviderID                     string
	ExternalTransactionID          string
	IdempotencyKey                 string
	PlayerID                       uuid.UUID
	WalletID                       uuid.UUID
	RoundID                        string
	GameID                         string
	Kind                           Kind
	Money                          money.Money
	ReferenceExternalTransactionID string
}

// NewRequest validates an external operation:
//   - identifiers are required, at most 128 chars (255 for the idempotency key)
//     and contain no whitespace or control characters;
//   - OPENING is rejected; LOSS requires exactly 0.00; every other kind requires a
//     positive amount;
//   - REFUND and ROLLBACK require a reference; BET and LOSS must not carry one;
//     WIN may reference a BET of the same round.
func NewRequest(r Request) (Request, error) {
	fields := []struct {
		name, value string
		max         int
	}{
		{"providerId", r.ProviderID, maxIdentifierLen},
		{"externalTransactionId", r.ExternalTransactionID, maxIdentifierLen},
		{"idempotencyKey", r.IdempotencyKey, maxIdempotencyKeyLen},
		{"roundId", r.RoundID, maxIdentifierLen},
		{"gameId", r.GameID, maxIdentifierLen},
	}
	for _, f := range fields {
		if err := validIdentifier(f.name, f.value, f.max); err != nil {
			return Request{}, err
		}
	}
	if r.PlayerID == uuid.Nil {
		return Request{}, derr.Wrapf(ErrInvalidRequest, "playerId is required")
	}
	if r.WalletID == uuid.Nil {
		return Request{}, derr.Wrapf(ErrInvalidRequest, "walletId is required")
	}
	if _, err := ParseExternalKind(string(r.Kind)); err != nil {
		return Request{}, err
	}
	if err := r.Money.Validate(); err != nil {
		return Request{}, err
	}
	if r.Money.IsNegative() {
		return Request{}, money.ErrNegativeAmount
	}
	if r.Kind == KindLoss {
		if !r.Money.IsZero() {
			return Request{}, derr.Wrapf(ErrInvalidAmount, "LOSS requires amount 0.00")
		}
	} else if !r.Money.IsPositive() {
		return Request{}, derr.Wrapf(ErrInvalidAmount, "%s requires a positive amount", r.Kind)
	}
	switch r.Kind {
	case KindRefund, KindRollback:
		if r.ReferenceExternalTransactionID == "" {
			return Request{}, ErrReferenceRequired
		}
	case KindBet, KindLoss:
		if r.ReferenceExternalTransactionID != "" {
			return Request{}, derr.Wrapf(ErrReferenceNotAllowed, "%s must not reference another transaction", r.Kind)
		}
	}
	if r.ReferenceExternalTransactionID != "" {
		if err := validIdentifier("referenceExternalTransactionId", r.ReferenceExternalTransactionID, maxIdentifierLen); err != nil {
			return Request{}, err
		}
		if r.ReferenceExternalTransactionID == r.ExternalTransactionID {
			return Request{}, derr.Wrapf(ErrInvalidRequest, "a transaction cannot reference itself")
		}
	}
	return r, nil
}

func validIdentifier(name, v string, max int) error {
	if v == "" {
		return derr.Wrapf(ErrInvalidRequest, "%s is required", name)
	}
	if len(v) > max {
		return derr.Wrapf(ErrInvalidRequest, "%s exceeds %d characters", name, max)
	}
	for _, r := range v {
		if unicode.IsSpace(r) || unicode.IsControl(r) || r == unicode.ReplacementChar {
			return derr.Wrapf(ErrInvalidRequest, "%s contains invalid characters", name)
		}
	}
	return nil
}

// PayloadHash is the deterministic SHA-256 (hex) of the canonical JSON of the
// business fields. The idempotency key and every transport metadata (headers,
// SQS messageId, occurredAt, correlation ids) are excluded, so HTTP and SQS
// produce the same hash for the same operation.
//
// Canonical JSON: an object with keys sorted lexicographically, no insignificant
// whitespace, no HTML escaping, all values as JSON strings:
//
//	gameId, kind, money.amount, money.currency, playerId, providerId,
//	externalTransactionId, referenceExternalTransactionId (only when present),
//	roundId, walletId
//
// Normalization: UUIDs are rendered in lowercase canonical 8-4-4-4-12 form (the
// parser accepts any case); money is the canonical "0.00" form, which is the only
// form the parser accepts; every other string is used verbatim.
func (r Request) PayloadHash() string {
	canonical := map[string]any{
		"providerId":            r.ProviderID,
		"externalTransactionId": r.ExternalTransactionID,
		"playerId":              r.PlayerID.String(),
		"walletId":              r.WalletID.String(),
		"roundId":               r.RoundID,
		"gameId":                r.GameID,
		"kind":                  string(r.Kind),
		"money": map[string]any{
			"amount":   r.Money.Amount(),
			"currency": r.Money.Currency().Code(),
		},
	}
	if r.ReferenceExternalTransactionID != "" {
		canonical["referenceExternalTransactionId"] = r.ReferenceExternalTransactionID
	}
	return hashCanonical(canonical)
}

// hashCanonical relies on encoding/json sorting map keys.
func hashCanonical(v map[string]any) string {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	// Only strings and nested maps of strings are encoded, so Encode cannot fail.
	_ = enc.Encode(v)
	sum := sha256.Sum256(bytes.TrimRight(buf.Bytes(), "\n"))
	return hex.EncodeToString(sum[:])
}
