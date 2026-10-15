package wager

import (
	"time"

	"github.com/google/uuid"

	"github.com/julioCAlmeida/backend-challenge-go/internal/domain/derr"
	"github.com/julioCAlmeida/backend-challenge-go/internal/domain/events"
	"github.com/julioCAlmeida/backend-challenge-go/internal/domain/money"
)

// Transaction is a wager transaction. State only changes through the explicit
// transition methods, which validate the state machine and record the matching
// integration events. Rehydrate never records events.
type Transaction struct {
	id     uuid.UUID
	origin Origin
	kind   Kind
	status Status

	providerID            string
	externalTransactionID string
	idempotencyKey        string
	payloadHash           string
	walletID              uuid.UUID
	playerID              uuid.UUID
	roundID               string
	gameID                string
	money                 money.Money
	referenceExternalID   string

	referenceTransactionID *uuid.UUID
	failureCode            FailureCode
	resultBalance          *money.Money
	referenceAttempts      int
	nextAttemptAt          *time.Time

	correlationID string
	causationID   string
	createdAt     time.Time
	updatedAt     time.Time
	completedAt   *time.Time

	recorded []events.Event
}

// Snapshot is the persisted shape of a transaction.
type Snapshot struct {
	ID                             uuid.UUID
	Origin                         Origin
	Kind                           Kind
	Status                         Status
	ProviderID                     string
	ExternalTransactionID          string
	IdempotencyKey                 string
	PayloadHash                    string
	WalletID                       uuid.UUID
	PlayerID                       uuid.UUID
	RoundID                        string
	GameID                         string
	Money                          money.Money
	ReferenceExternalTransactionID string
	ReferenceTransactionID         *uuid.UUID
	FailureCode                    FailureCode
	ResultBalance                  *money.Money
	ReferenceAttempts              int
	NextAttemptAt                  *time.Time
	CorrelationID                  string
	CausationID                    string
	CreatedAt                      time.Time
	UpdatedAt                      time.Time
	CompletedAt                    *time.Time
}

// NewExternal creates a PENDING transaction from a validated request.
func NewExternal(id uuid.UUID, req Request, meta events.Meta, now time.Time) (*Transaction, error) {
	if id == uuid.Nil {
		return nil, derr.Wrapf(ErrInvalidTransaction, "id is required")
	}
	if now.IsZero() {
		return nil, derr.Wrapf(ErrInvalidTransaction, "timestamp is required")
	}
	validated, err := NewRequest(req)
	if err != nil {
		return nil, err
	}
	now = now.UTC()
	return &Transaction{
		id: id, origin: OriginExternal, kind: validated.Kind, status: StatusPending,
		providerID: validated.ProviderID, externalTransactionID: validated.ExternalTransactionID,
		idempotencyKey: validated.IdempotencyKey, payloadHash: validated.PayloadHash(),
		walletID: validated.WalletID, playerID: validated.PlayerID, roundID: validated.RoundID,
		gameID: validated.GameID, money: validated.Money, referenceExternalID: validated.ReferenceExternalTransactionID,
		correlationID: meta.CorrelationID, causationID: meta.CausationID, createdAt: now, updatedAt: now,
	}, nil
}

// NewOpening creates the internal PENDING OPENING credit of a wallet. External
// metadata (provider, external id, key, hash, round, game, reference) does not
// apply to this origin.
func NewOpening(id, walletID, playerID uuid.UUID, amount money.Money, meta events.Meta, now time.Time) (*Transaction, error) {
	if id == uuid.Nil || walletID == uuid.Nil || playerID == uuid.Nil {
		return nil, derr.Wrapf(ErrInvalidTransaction, "opening requires id, wallet and player")
	}
	if now.IsZero() {
		return nil, derr.Wrapf(ErrInvalidTransaction, "timestamp is required")
	}
	if err := amount.Validate(); err != nil {
		return nil, err
	}
	if !amount.IsPositive() {
		return nil, derr.Wrapf(ErrInvalidAmount, "OPENING requires a positive amount")
	}
	now = now.UTC()
	return &Transaction{
		id: id, origin: OriginInternal, kind: KindOpening, status: StatusPending,
		walletID: walletID, playerID: playerID, money: amount,
		correlationID: meta.CorrelationID, causationID: meta.CausationID, createdAt: now, updatedAt: now,
	}, nil
}

// Rehydrate rebuilds a transaction from storage. It validates consistency but
// never applies movements, transitions or events.
func Rehydrate(s Snapshot) (*Transaction, error) {
	if s.ID == uuid.Nil || s.WalletID == uuid.Nil || s.PlayerID == uuid.Nil {
		return nil, derr.Wrapf(ErrInvalidTransaction, "identifiers are required")
	}
	if !s.Status.Valid() {
		return nil, derr.Wrapf(ErrInvalidTransaction, "unknown status %q", s.Status)
	}
	if _, err := ParseKind(string(s.Kind)); err != nil {
		return nil, err
	}
	if err := s.Money.Validate(); err != nil {
		return nil, err
	}
	switch s.Origin {
	case OriginInternal:
		if s.Kind != KindOpening || s.ProviderID != "" || s.ExternalTransactionID != "" || s.IdempotencyKey != "" {
			return nil, derr.Wrapf(ErrInvalidTransaction, "internal transaction carries external metadata")
		}
	case OriginExternal:
		if s.Kind == KindOpening || s.ProviderID == "" || s.ExternalTransactionID == "" || s.IdempotencyKey == "" || s.PayloadHash == "" {
			return nil, derr.Wrapf(ErrInvalidTransaction, "external transaction misses metadata")
		}
	default:
		return nil, derr.Wrapf(ErrInvalidTransaction, "unknown origin %q", s.Origin)
	}
	if (s.Status == StatusRejected || s.Status == StatusFailed) && s.FailureCode == "" {
		return nil, derr.Wrapf(ErrInvalidTransaction, "%s transaction without failure code", s.Status)
	}
	return &Transaction{
		id: s.ID, origin: s.Origin, kind: s.Kind, status: s.Status,
		providerID: s.ProviderID, externalTransactionID: s.ExternalTransactionID,
		idempotencyKey: s.IdempotencyKey, payloadHash: s.PayloadHash, walletID: s.WalletID,
		playerID: s.PlayerID, roundID: s.RoundID, gameID: s.GameID, money: s.Money,
		referenceExternalID: s.ReferenceExternalTransactionID, referenceTransactionID: s.ReferenceTransactionID,
		failureCode: s.FailureCode, resultBalance: s.ResultBalance, referenceAttempts: s.ReferenceAttempts,
		nextAttemptAt: s.NextAttemptAt, correlationID: s.CorrelationID, causationID: s.CausationID,
		createdAt: s.CreatedAt.UTC(), updatedAt: s.UpdatedAt.UTC(), completedAt: s.CompletedAt,
	}, nil
}

// Snapshot exposes the persisted shape.
func (t *Transaction) Snapshot() Snapshot {
	return Snapshot{
		ID: t.id, Origin: t.origin, Kind: t.kind, Status: t.status, ProviderID: t.providerID,
		ExternalTransactionID: t.externalTransactionID, IdempotencyKey: t.idempotencyKey,
		PayloadHash: t.payloadHash, WalletID: t.walletID, PlayerID: t.playerID, RoundID: t.roundID,
		GameID: t.gameID, Money: t.money, ReferenceExternalTransactionID: t.referenceExternalID,
		ReferenceTransactionID: t.referenceTransactionID, FailureCode: t.failureCode,
		ResultBalance: t.resultBalance, ReferenceAttempts: t.referenceAttempts, NextAttemptAt: t.nextAttemptAt,
		CorrelationID: t.correlationID, CausationID: t.causationID, CreatedAt: t.createdAt,
		UpdatedAt: t.updatedAt, CompletedAt: t.completedAt,
	}
}

func (t *Transaction) ID() uuid.UUID                      { return t.id }
func (t *Transaction) Origin() Origin                     { return t.origin }
func (t *Transaction) Kind() Kind                         { return t.kind }
func (t *Transaction) Status() Status                     { return t.status }
func (t *Transaction) ProviderID() string                 { return t.providerID }
func (t *Transaction) ExternalTransactionID() string      { return t.externalTransactionID }
func (t *Transaction) IdempotencyKey() string             { return t.idempotencyKey }
func (t *Transaction) PayloadHash() string                { return t.payloadHash }
func (t *Transaction) WalletID() uuid.UUID                { return t.walletID }
func (t *Transaction) PlayerID() uuid.UUID                { return t.playerID }
func (t *Transaction) RoundID() string                    { return t.roundID }
func (t *Transaction) GameID() string                     { return t.gameID }
func (t *Transaction) Money() money.Money                 { return t.money }
func (t *Transaction) ReferenceExternalID() string        { return t.referenceExternalID }
func (t *Transaction) ReferenceTransactionID() *uuid.UUID { return t.referenceTransactionID }
func (t *Transaction) FailureCode() FailureCode           { return t.failureCode }
func (t *Transaction) ResultBalance() *money.Money        { return t.resultBalance }
func (t *Transaction) ReferenceAttempts() int             { return t.referenceAttempts }
func (t *Transaction) NextAttemptAt() *time.Time          { return t.nextAttemptAt }
func (t *Transaction) CorrelationID() string              { return t.correlationID }
func (t *Transaction) CreatedAt() time.Time               { return t.createdAt }
func (t *Transaction) UpdatedAt() time.Time               { return t.updatedAt }
func (t *Transaction) CompletedAt() *time.Time            { return t.completedAt }
func (t *Transaction) Meta() events.Meta {
	return events.Meta{CorrelationID: t.correlationID, CausationID: t.causationID}
}

// NeedsReference reports whether the kind/request depends on another transaction.
func (t *Transaction) NeedsReference() bool { return t.referenceExternalID != "" }

// PullEvents returns and clears the events recorded by transitions.
func (t *Transaction) PullEvents() []events.Event {
	out := t.recorded
	t.recorded = nil
	return out
}

func (t *Transaction) transition(to Status, now time.Time) error {
	allowed := false
	switch t.status {
	case StatusPending:
		allowed = to == StatusPendingReference || to.Terminal()
	case StatusPendingReference:
		allowed = to == StatusPendingReference || to.Terminal()
	}
	if !allowed {
		return derr.Wrapf(ErrInvalidTransition, "%s -> %s", t.status, to)
	}
	t.status = to
	t.updatedAt = now.UTC()
	if to.Terminal() {
		at := now.UTC()
		t.completedAt = &at
		t.nextAttemptAt = nil
	}
	return nil
}

// markProcessed is the PENDING|PENDING_REFERENCE -> PROCESSED transition.
func (t *Transaction) markProcessed(balance money.Money, ref *Transaction, now time.Time) error {
	if err := t.transition(StatusProcessed, now); err != nil {
		return err
	}
	b := balance
	t.resultBalance = &b
	d := events.WagerTransactionProcessedData{
		TransactionID: t.id, Origin: string(t.origin), Kind: string(t.kind), WalletID: t.walletID,
		PlayerID: t.playerID, Money: t.money, BalanceAfter: balance, ProviderID: t.providerID,
		ExternalTransactionID: t.externalTransactionID, RoundID: t.roundID, GameID: t.gameID,
		ReferenceExternalTransactionID: t.referenceExternalID,
	}
	if ref != nil {
		id := ref.id
		t.referenceTransactionID = &id
		d.ReferenceTransactionID = &id
	}
	t.recorded = append(t.recorded, events.NewWagerTransactionProcessed(t.Meta(), now, d))
	return nil
}

// MarkRejected is the PENDING|PENDING_REFERENCE -> REJECTED transition. balance is
// the balance observed at rejection time, returned to the provider on replays;
// nil when it must not be disclosed (wallet/player mismatch).
func (t *Transaction) MarkRejected(code FailureCode, balance *money.Money, now time.Time) error {
	if code == "" {
		return derr.Wrapf(ErrInvalidTransition, "rejection requires a failure code")
	}
	if err := t.transition(StatusRejected, now); err != nil {
		return err
	}
	t.failureCode = code
	if balance != nil {
		b := *balance
		t.resultBalance = &b
	}
	t.recorded = append(t.recorded, events.NewWagerTransactionRejected(t.Meta(), now, events.WagerTransactionRejectedData{
		TransactionID: t.id, Kind: string(t.kind), WalletID: t.walletID, PlayerID: t.playerID, Money: t.money,
		ProviderID: t.providerID, ExternalTransactionID: t.externalTransactionID, RoundID: t.roundID,
		GameID: t.gameID, ReferenceExternalTransactionID: t.referenceExternalID, FailureCode: string(code),
	}))
	return nil
}

// MarkPendingReference records that the reference is not available yet. The first
// call (PENDING -> PENDING_REFERENCE) emits WagerTransactionPendingReference; later
// calls only reschedule the next attempt.
func (t *Transaction) MarkPendingReference(next time.Time, now time.Time) error {
	if t.referenceExternalID == "" {
		return derr.Wrapf(ErrInvalidTransition, "transaction has no reference")
	}
	first := t.status == StatusPending
	if err := t.transition(StatusPendingReference, now); err != nil {
		return err
	}
	n := next.UTC()
	t.nextAttemptAt = &n
	if first {
		t.recorded = append(t.recorded, events.NewWagerTransactionPendingReference(t.Meta(), now, n, events.WagerTransactionPendingReferenceData{
			TransactionID: t.id, Kind: string(t.kind), WalletID: t.walletID, PlayerID: t.playerID, Money: t.money,
			ProviderID: t.providerID, ExternalTransactionID: t.externalTransactionID,
			ReferenceExternalTransactionID: t.referenceExternalID,
		}))
	} else {
		t.referenceAttempts++
	}
	return nil
}

// MarkFailed records a permanent infrastructure failure for audit.
func (t *Transaction) MarkFailed(code FailureCode, now time.Time) error {
	if code == "" {
		return derr.Wrapf(ErrInvalidTransition, "failure requires a code")
	}
	if err := t.transition(StatusFailed, now); err != nil {
		return err
	}
	t.failureCode = code
	return nil
}
