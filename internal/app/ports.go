// Package app contains the use cases. It depends on the domain and on the ports
// declared here; adapters (PostgreSQL, SQS, HTTP) implement or call them.
package app

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/julioCAlmeida/backend-challenge-go/internal/domain/derr"
	"github.com/julioCAlmeida/backend-challenge-go/internal/domain/events"
	"github.com/julioCAlmeida/backend-challenge-go/internal/domain/wager"
	"github.com/julioCAlmeida/backend-challenge-go/internal/domain/wallet"
)

// Infrastructure error classes. Adapters wrap driver errors with these so the
// use cases and the transports can decide between retrying and failing.
var (
	// ErrTransient: temporary unavailability (connection lost, timeout, lock
	// timeout, broker throttling). Safe to retry the whole operation.
	ErrTransient = errors.New("transient infrastructure failure")
	// ErrConcurrencyConflict: deadlock, serialization failure or a lost
	// optimistic version check. Retried a bounded number of times.
	ErrConcurrencyConflict = errors.New("concurrency conflict")
)

var (
	ErrInboxHashMismatch = derr.New(derr.KindConflict, "MESSAGE_HASH_MISMATCH", "message id was already received with a different payload")
	ErrInvalidCursor     = derr.New(derr.KindValidation, "INVALID_CURSOR", "cursor is invalid")
)

// IsRetryable reports whether err is a transient or concurrency failure.
func IsRetryable(err error) bool {
	return errors.Is(err, ErrTransient) || errors.Is(err, ErrConcurrencyConflict)
}

// Clock abstracts time for tests. Implementations return UTC truncated to
// microseconds (PostgreSQL precision) so persisted and in-memory values match.
type Clock interface{ Now() time.Time }

// IDGenerator creates time-ordered identifiers (UUIDv7).
type IDGenerator interface{ New() uuid.UUID }

// UnitOfWork delimits SQL transactions. Every repository obtained from Tx runs
// in the same transaction; the transaction commits only if fn returns nil.
type UnitOfWork interface {
	Within(ctx context.Context, fn func(ctx context.Context, tx Tx) error) error
	// Snapshot runs fn in a READ ONLY REPEATABLE READ transaction (consistent view).
	Snapshot(ctx context.Context, fn func(ctx context.Context, tx Tx) error) error
}

// Tx gives access to the repositories bound to one SQL transaction.
type Tx interface {
	Wallets() WalletRepository
	Transactions() TransactionRepository
	Ledger() LedgerRepository
	Outbox() OutboxWriter
	Inbox() InboxRepository
}

type WalletRepository interface {
	// Insert fails with wallet.ErrWalletExists when (playerId, currency) exists.
	Insert(ctx context.Context, w *wallet.Wallet) error
	Get(ctx context.Context, id uuid.UUID) (*wallet.Wallet, error)
	// GetForUpdate locks the wallet row (SELECT ... FOR UPDATE).
	GetForUpdate(ctx context.Context, id uuid.UUID) (*wallet.Wallet, error)
	// TryGetForUpdate is GetForUpdate with SKIP LOCKED; ok=false when busy.
	TryGetForUpdate(ctx context.Context, id uuid.UUID) (w *wallet.Wallet, ok bool, err error)
	// UpdateBalance persists balance and version conditioned on the persisted
	// version; returns ErrConcurrencyConflict when another writer won.
	UpdateBalance(ctx context.Context, w *wallet.Wallet) error
}

type TransactionRepository interface {
	// Insert stores a new transaction and fails on any unique violation.
	Insert(ctx context.Context, t *wager.Transaction) error
	// InsertIfAbsent stores the transaction unless the idempotency key or the
	// external id is already taken (ON CONFLICT DO NOTHING).
	InsertIfAbsent(ctx context.Context, t *wager.Transaction) (inserted bool, err error)
	Update(ctx context.Context, t *wager.Transaction) error
	Get(ctx context.Context, id uuid.UUID) (*wager.Transaction, error)
	GetForUpdate(ctx context.Context, id uuid.UUID) (*wager.Transaction, error)
	// FindByKeyOrExternalID returns, in one consistent read, the transactions of
	// the provider matching the idempotency key or the external id (0 to 2 rows).
	FindByKeyOrExternalID(ctx context.Context, providerID, key, externalID string) ([]*wager.Transaction, error)
	FindByExternalID(ctx context.Context, providerID, externalID string) (*wager.Transaction, error)
	HasProcessedReversal(ctx context.Context, referenceID uuid.UUID) (bool, error)
	// WakeWaiting moves the next attempt of transactions waiting for
	// (providerId, externalId) to `at`.
	WakeWaiting(ctx context.Context, providerID, externalID string, at time.Time) error
	// DuePending lists PENDING/PENDING_REFERENCE transactions whose next attempt is due.
	DuePending(ctx context.Context, now time.Time, limit int) ([]PendingRef, error)
}

// PendingRef identifies a pending transaction to resume.
type PendingRef struct {
	TransactionID uuid.UUID
	WalletID      uuid.UUID
}

// LedgerRow is a persisted entry with its stable ordering key.
type LedgerRow struct {
	Seq   int64
	Entry wallet.LedgerEntry
}

// LedgerTotals is the reconstruction of a wallet balance from its ledger.
type LedgerTotals struct {
	CreditsMinor int64
	DebitsMinor  int64
	Entries      int64
}

type LedgerRepository interface {
	Append(ctx context.Context, e wallet.LedgerEntry) error
	List(ctx context.Context, walletID uuid.UUID, afterSeq int64, limit int) ([]LedgerRow, error)
	Totals(ctx context.Context, walletID uuid.UUID) (LedgerTotals, error)
}

type OutboxWriter interface {
	Append(ctx context.Context, evs ...events.Event) error
}

// InboxMessage is the durable identity of a consumed message.
type InboxMessage struct {
	Consumer    string
	MessageID   string
	PayloadHash string
	ReceivedAt  time.Time
}

// InboxRecord is a persisted inbox row.
type InboxRecord struct {
	InboxMessage
	TransactionID *uuid.UUID
	Outcome       string
	CompletedAt   *time.Time
}

type InboxRepository interface {
	// Register inserts the message; when it already exists the stored record is
	// returned instead (after any concurrent writer committed).
	Register(ctx context.Context, m InboxMessage) (existing *InboxRecord, err error)
	Complete(ctx context.Context, consumer, messageID string, transactionID *uuid.UUID, outcome string, at time.Time) error
}

// Metrics is the observability port used by the use cases.
type Metrics interface {
	TransactionCompleted(channel, kind, status string)
	IdempotentReplay(channel string)
	IdempotencyConflict(channel string)
	DuplicateMessage(consumer string)
	ConcurrencyConflict(operation string)
	Retry(component string)
	ProcessingDuration(channel string, d time.Duration)
	ReconciliationChecked(consistent bool)
	PendingReferenceExpired()
}

// NopMetrics discards every measurement.
type NopMetrics struct{}

func (NopMetrics) TransactionCompleted(string, string, string) {}
func (NopMetrics) IdempotentReplay(string)                     {}
func (NopMetrics) IdempotencyConflict(string)                  {}
func (NopMetrics) DuplicateMessage(string)                     {}
func (NopMetrics) ConcurrencyConflict(string)                  {}
func (NopMetrics) Retry(string)                                {}
func (NopMetrics) ProcessingDuration(string, time.Duration)    {}
func (NopMetrics) ReconciliationChecked(bool)                  {}
func (NopMetrics) PendingReferenceExpired()                    {}
