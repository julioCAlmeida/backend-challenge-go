// Package contract holds the wire formats shared by HTTP and SQS and their
// conversion to domain values, so both entry points validate identically.
package contract

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"time"

	"github.com/google/uuid"

	"github.com/julioCAlmeida/backend-challenge-go/internal/domain/derr"
	"github.com/julioCAlmeida/backend-challenge-go/internal/domain/money"
	"github.com/julioCAlmeida/backend-challenge-go/internal/domain/wager"
	"github.com/julioCAlmeida/backend-challenge-go/internal/domain/wallet"
)

var ErrMalformed = derr.New(derr.KindValidation, "MALFORMED_REQUEST", "request body is not valid JSON for this contract")

// Money is the external money format {"amount":"25.00","currency":"BRL"}.
// The amount must be a JSON string; numbers are rejected so values never go
// through float64.
type Money struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

func (m *Money) UnmarshalJSON(b []byte) error {
	var raw struct {
		Amount   json.RawMessage `json:"amount"`
		Currency json.RawMessage `json:"currency"`
	}
	if err := strictDecode(b, &raw); err != nil {
		return err
	}
	if len(raw.Amount) == 0 || raw.Amount[0] != '"' {
		return derr.Wrapf(money.ErrInvalidAmount, "money.amount must be a JSON string")
	}
	if len(raw.Currency) == 0 || raw.Currency[0] != '"' {
		return derr.Wrapf(money.ErrInvalidCurrency, "money.currency must be a JSON string")
	}
	if err := json.Unmarshal(raw.Amount, &m.Amount); err != nil {
		return derr.Wrapf(money.ErrInvalidAmount, "money.amount is malformed")
	}
	return json.Unmarshal(raw.Currency, &m.Currency)
}

// ToDomain parses an external (non-negative) amount.
func (m *Money) ToDomain() (money.Money, error) {
	if m == nil {
		return money.Money{}, derr.Wrapf(money.ErrInvalidAmount, "money is required")
	}
	return money.ParseNonNegative(m.Amount, m.Currency)
}

// FromDomain renders a domain Money.
func FromDomain(m money.Money) Money {
	return Money{Amount: m.Amount(), Currency: m.Currency().Code()}
}

// WagerFields are the business fields of an operation (HTTP body and SQS data).
type WagerFields struct {
	ProviderID                     string `json:"providerId"`
	ExternalTransactionID          string `json:"externalTransactionId"`
	PlayerID                       string `json:"playerId"`
	WalletID                       string `json:"walletId"`
	RoundID                        string `json:"roundId"`
	GameID                         string `json:"gameId"`
	Kind                           string `json:"kind"`
	Money                          *Money `json:"money"`
	ReferenceExternalTransactionID string `json:"referenceExternalTransactionId,omitempty"`
}

// ToRequest validates the fields and builds the domain request.
func (f WagerFields) ToRequest(idempotencyKey string) (wager.Request, error) {
	player, err := parseUUID("playerId", f.PlayerID)
	if err != nil {
		return wager.Request{}, err
	}
	walletID, err := parseUUID("walletId", f.WalletID)
	if err != nil {
		return wager.Request{}, err
	}
	kind, err := wager.ParseExternalKind(f.Kind)
	if err != nil {
		return wager.Request{}, err
	}
	m, err := f.Money.ToDomain()
	if err != nil {
		return wager.Request{}, err
	}
	return wager.NewRequest(wager.Request{
		ProviderID: f.ProviderID, ExternalTransactionID: f.ExternalTransactionID, IdempotencyKey: idempotencyKey,
		PlayerID: player, WalletID: walletID, RoundID: f.RoundID, GameID: f.GameID, Kind: kind, Money: m,
		ReferenceExternalTransactionID: f.ReferenceExternalTransactionID,
	})
}

func parseUUID(field, v string) (uuid.UUID, error) {
	id, err := uuid.Parse(v)
	if err != nil || len(v) != 36 {
		return uuid.Nil, derr.Wrapf(wager.ErrInvalidRequest, "%s must be a UUID in 8-4-4-4-12 form", field)
	}
	return id, nil
}

// MessageTypeWagerTransactionRequested is the only accepted SQS message type.
const MessageTypeWagerTransactionRequested = "WagerTransactionRequested"

// WagerMessage is the SQS envelope.
type WagerMessage struct {
	MessageID  string           `json:"messageId"`
	Type       string           `json:"type"`
	OccurredAt string           `json:"occurredAt"`
	Data       WagerMessageData `json:"data"`
}

// WagerMessageData is WagerFields plus the idempotency key.
type WagerMessageData struct {
	WagerFields
	IdempotencyKey string `json:"idempotencyKey"`
}

// ParseWagerMessage decodes and validates an SQS body. The returned hash
// identifies the message content for the inbox: SHA-256 of type, idempotency
// key and the business payload hash (transport fields such as occurredAt are
// excluded so a re-sent copy of the same message matches).
func ParseWagerMessage(body []byte) (WagerMessage, wager.Request, string, error) {
	var m WagerMessage
	if err := strictDecode(body, &m); err != nil {
		return m, wager.Request{}, "", err
	}
	if m.MessageID == "" || len(m.MessageID) > 128 {
		return m, wager.Request{}, "", derr.Wrapf(ErrMalformed, "messageId is required (max 128 chars)")
	}
	if m.Type != MessageTypeWagerTransactionRequested {
		return m, wager.Request{}, "", derr.Wrapf(ErrMalformed, "unsupported message type %q", m.Type)
	}
	if m.OccurredAt != "" {
		if _, err := time.Parse(time.RFC3339Nano, m.OccurredAt); err != nil {
			return m, wager.Request{}, "", derr.Wrapf(ErrMalformed, "occurredAt must be RFC 3339")
		}
	}
	req, err := m.Data.ToRequest(m.Data.IdempotencyKey)
	if err != nil {
		return m, wager.Request{}, "", err
	}
	sum := sha256.Sum256([]byte(m.Type + "\n" + req.IdempotencyKey + "\n" + req.PayloadHash()))
	return m, req, hex.EncodeToString(sum[:]), nil
}

// DecodeStrict decodes exactly one JSON value, rejecting unknown fields.
func DecodeStrict(r io.Reader, v any) error {
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return wrapDecode(err)
	}
	if dec.More() {
		return derr.Wrapf(ErrMalformed, "trailing data after JSON value")
	}
	if _, err := dec.Token(); err != io.EOF {
		return derr.Wrapf(ErrMalformed, "trailing data after JSON value")
	}
	return nil
}

func strictDecode(b []byte, v any) error { return DecodeStrict(bytes.NewReader(b), v) }

func wrapDecode(err error) error {
	var de *derr.Error
	if errors.As(err, &de) {
		return de
	}
	return derr.Wrapf(ErrMalformed, "%v", err)
}

// WalletResponse is the wallet representation.
type WalletResponse struct {
	ID       uuid.UUID `json:"id"`
	PlayerID uuid.UUID `json:"playerId"`
	Balance  Money     `json:"balance"`
	Version  int64     `json:"version"`
}

func NewWalletResponse(s wallet.Snapshot) WalletResponse {
	return WalletResponse{ID: s.ID, PlayerID: s.PlayerID, Balance: FromDomain(s.Balance), Version: s.Version}
}
