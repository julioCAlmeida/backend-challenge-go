// Package events defines the integration events emitted by the domain.
//
// Each event has a concrete type whose constructor fixes eventType and version.
// Events are serialized once, when written to the outbox, and that snapshot is
// what gets published (and republished with the same eventId).
package events

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"

	"github.com/julioCAlmeida/backend-challenge-go/internal/domain/money"
)

const (
	TypeWagerTransactionProcessed        = "WagerTransactionProcessed"
	TypeWagerTransactionRejected         = "WagerTransactionRejected"
	TypeWalletBalanceChanged             = "WalletBalanceChanged"
	TypeWagerTransactionPendingReference = "WagerTransactionPendingReference"

	AggregateWagerTransaction = "WagerTransaction"
	AggregateWallet           = "Wallet"
)

// Meta carries tracing identifiers propagated from the triggering request.
type Meta struct {
	CorrelationID string
	CausationID   string
}

// Event is implemented by every concrete event type.
type Event interface {
	ID() uuid.UUID
	Type() string
	AggregateType() string
	AggregateID() string
	// PartitionKey groups events that must keep their relative order
	// (the wallet id); it becomes the SQS MessageGroupId.
	PartitionKey() string
	OccurredAt() time.Time
	// Payload is the immutable JSON snapshot stored in the outbox.
	Payload() ([]byte, error)
}

// Envelope is the wire format shared by every event.
type Envelope[T any] struct {
	EventID       uuid.UUID `json:"eventId"`
	EventType     string    `json:"eventType"`
	AggregateRef  string    `json:"aggregateId"`
	CorrelationID string    `json:"correlationId"`
	CausationID   *string   `json:"causationId,omitempty"`
	OccurredAtUTC string    `json:"occurredAt"`
	Version       int       `json:"version"`
	Data          T         `json:"data"`

	aggregateType string
	partitionKey  string
	occurredAt    time.Time
}

func newEnvelope[T any](eventType, aggregateType, aggregateID, partitionKey string, version int, meta Meta, at time.Time, data T) Envelope[T] {
	at = at.UTC()
	var causation *string
	if meta.CausationID != "" {
		c := meta.CausationID
		causation = &c
	}
	return Envelope[T]{
		EventID: uuid.Must(uuid.NewV7()), EventType: eventType, AggregateRef: aggregateID,
		CorrelationID: meta.CorrelationID, CausationID: causation,
		OccurredAtUTC: at.Format(time.RFC3339Nano), Version: version, Data: data,
		aggregateType: aggregateType, partitionKey: partitionKey, occurredAt: at,
	}
}

func (e Envelope[T]) ID() uuid.UUID            { return e.EventID }
func (e Envelope[T]) Type() string             { return e.EventType }
func (e Envelope[T]) AggregateType() string    { return e.aggregateType }
func (e Envelope[T]) AggregateID() string      { return e.AggregateRef }
func (e Envelope[T]) PartitionKey() string     { return e.partitionKey }
func (e Envelope[T]) OccurredAt() time.Time    { return e.occurredAt }
func (e Envelope[T]) Payload() ([]byte, error) { return json.Marshal(e) }

// WagerTransactionProcessedData is the payload of WagerTransactionProcessed.
// External metadata is omitted for the internal OPENING origin.
type WagerTransactionProcessedData struct {
	TransactionID                  uuid.UUID   `json:"transactionId"`
	Origin                         string      `json:"origin"`
	Kind                           string      `json:"kind"`
	WalletID                       uuid.UUID   `json:"walletId"`
	PlayerID                       uuid.UUID   `json:"playerId"`
	Money                          money.Money `json:"money"`
	BalanceAfter                   money.Money `json:"balanceAfter"`
	ProviderID                     string      `json:"providerId,omitempty"`
	ExternalTransactionID          string      `json:"externalTransactionId,omitempty"`
	RoundID                        string      `json:"roundId,omitempty"`
	GameID                         string      `json:"gameId,omitempty"`
	ReferenceExternalTransactionID string      `json:"referenceExternalTransactionId,omitempty"`
	ReferenceTransactionID         *uuid.UUID  `json:"referenceTransactionId,omitempty"`
	ProcessedAt                    string      `json:"processedAt"`
}

// WagerTransactionProcessed signals a successful operation, LOSS included.
type WagerTransactionProcessed struct {
	Envelope[WagerTransactionProcessedData]
}

func NewWagerTransactionProcessed(meta Meta, at time.Time, d WagerTransactionProcessedData) WagerTransactionProcessed {
	d.ProcessedAt = at.UTC().Format(time.RFC3339Nano)
	return WagerTransactionProcessed{newEnvelope(TypeWagerTransactionProcessed, AggregateWagerTransaction,
		d.TransactionID.String(), d.WalletID.String(), 1, meta, at, d)}
}

// WagerTransactionRejectedData is the payload of WagerTransactionRejected.
type WagerTransactionRejectedData struct {
	TransactionID                  uuid.UUID   `json:"transactionId"`
	Kind                           string      `json:"kind"`
	WalletID                       uuid.UUID   `json:"walletId"`
	PlayerID                       uuid.UUID   `json:"playerId"`
	Money                          money.Money `json:"money"`
	ProviderID                     string      `json:"providerId"`
	ExternalTransactionID          string      `json:"externalTransactionId"`
	RoundID                        string      `json:"roundId"`
	GameID                         string      `json:"gameId"`
	ReferenceExternalTransactionID string      `json:"referenceExternalTransactionId,omitempty"`
	FailureCode                    string      `json:"failureCode"`
	RejectedAt                     string      `json:"rejectedAt"`
}

// WagerTransactionRejected signals a definitive business rejection.
type WagerTransactionRejected struct {
	Envelope[WagerTransactionRejectedData]
}

func NewWagerTransactionRejected(meta Meta, at time.Time, d WagerTransactionRejectedData) WagerTransactionRejected {
	d.RejectedAt = at.UTC().Format(time.RFC3339Nano)
	return WagerTransactionRejected{newEnvelope(TypeWagerTransactionRejected, AggregateWagerTransaction,
		d.TransactionID.String(), d.WalletID.String(), 1, meta, at, d)}
}

// WalletBalanceChangedData is the payload of WalletBalanceChanged.
type WalletBalanceChangedData struct {
	WalletID      uuid.UUID   `json:"walletId"`
	TransactionID uuid.UUID   `json:"transactionId"`
	Direction     string      `json:"direction"`
	Money         money.Money `json:"money"`
	BalanceBefore money.Money `json:"balanceBefore"`
	BalanceAfter  money.Money `json:"balanceAfter"`
	WalletVersion int64       `json:"walletVersion"`
}

// WalletBalanceChanged signals an effective balance change.
type WalletBalanceChanged struct {
	Envelope[WalletBalanceChangedData]
}

func NewWalletBalanceChanged(meta Meta, at time.Time, d WalletBalanceChangedData) WalletBalanceChanged {
	return WalletBalanceChanged{newEnvelope(TypeWalletBalanceChanged, AggregateWallet,
		d.WalletID.String(), d.WalletID.String(), 1, meta, at, d)}
}

// WagerTransactionPendingReferenceData is the payload of WagerTransactionPendingReference.
type WagerTransactionPendingReferenceData struct {
	TransactionID                  uuid.UUID   `json:"transactionId"`
	Kind                           string      `json:"kind"`
	WalletID                       uuid.UUID   `json:"walletId"`
	PlayerID                       uuid.UUID   `json:"playerId"`
	Money                          money.Money `json:"money"`
	ProviderID                     string      `json:"providerId"`
	ExternalTransactionID          string      `json:"externalTransactionId"`
	ReferenceExternalTransactionID string      `json:"referenceExternalTransactionId"`
	NextAttemptAt                  string      `json:"nextAttemptAt"`
}

// WagerTransactionPendingReference signals that an operation waits for its reference.
type WagerTransactionPendingReference struct {
	Envelope[WagerTransactionPendingReferenceData]
}

func NewWagerTransactionPendingReference(meta Meta, at, nextAttempt time.Time, d WagerTransactionPendingReferenceData) WagerTransactionPendingReference {
	d.NextAttemptAt = nextAttempt.UTC().Format(time.RFC3339Nano)
	return WagerTransactionPendingReference{newEnvelope(TypeWagerTransactionPendingReference, AggregateWagerTransaction,
		d.TransactionID.String(), d.WalletID.String(), 1, meta, at, d)}
}
