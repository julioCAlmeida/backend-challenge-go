package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/julioCAlmeida/backend-challenge-go/internal/domain/derr"
	"github.com/julioCAlmeida/backend-challenge-go/internal/domain/events"
	"github.com/julioCAlmeida/backend-challenge-go/internal/domain/wager"
	"github.com/julioCAlmeida/backend-challenge-go/internal/domain/wallet"
)

// Channels used in metrics and logs.
const (
	ChannelHTTP   = "http"
	ChannelSQS    = "sqs"
	ChannelWorker = "worker"
)

// ProcessCommand is the input shared by HTTP and SQS.
type ProcessCommand struct {
	Request wager.Request
	Meta    events.Meta
	Channel string
	// Inbox is set by the SQS consumer: the message is registered and completed
	// in the same SQL transaction as the financial changes.
	Inbox *InboxMessage
}

// ProcessResult is the persisted outcome of an operation.
type ProcessResult struct {
	Transaction wager.Snapshot
	// Replay is true when the operation had already been recorded and the stored
	// result is returned without reapplying it.
	Replay bool
	// DuplicateMessage is true when the inbox had already completed this message.
	DuplicateMessage bool
}

// WageringConfig tunes the use case.
type WageringConfig struct {
	ConflictRetries int
	Schedule        wager.RetrySchedule
}

// WageringService processes provider operations.
type WageringService struct {
	uow     UnitOfWork
	clock   Clock
	ids     IDGenerator
	metrics Metrics
	log     *slog.Logger
	cfg     WageringConfig
}

func NewWageringService(uow UnitOfWork, clock Clock, ids IDGenerator, metrics Metrics, log *slog.Logger, cfg WageringConfig) *WageringService {
	if cfg.ConflictRetries < 1 {
		cfg.ConflictRetries = 1
	}
	return &WageringService{uow: uow, clock: clock, ids: ids, metrics: metrics, log: log, cfg: cfg}
}

// Process records and applies an operation exactly once. Concurrent or repeated
// deliveries (HTTP, SQS or both) converge on the same persisted result.
func (s *WageringService) Process(ctx context.Context, cmd ProcessCommand) (ProcessResult, error) {
	start := time.Now()
	req, err := wager.NewRequest(cmd.Request)
	if err != nil {
		return ProcessResult{}, err
	}
	var res ProcessResult
	err = retryConflicts(ctx, s.cfg.ConflictRetries, func() { s.metrics.ConcurrencyConflict("process") }, func() error {
		var txErr error
		res, txErr = s.processOnce(ctx, req, cmd)
		return txErr
	})
	s.metrics.ProcessingDuration(cmd.Channel, time.Since(start))
	if err != nil {
		if errors.Is(err, wager.ErrIdempotencyConflict) || errors.Is(err, wager.ErrExternalIDConflict) {
			s.metrics.IdempotencyConflict(cmd.Channel)
		}
		return ProcessResult{}, err
	}
	switch {
	case res.DuplicateMessage:
		s.metrics.DuplicateMessage(cmd.Inbox.Consumer)
	case res.Replay:
		s.metrics.IdempotentReplay(cmd.Channel)
	default:
		s.metrics.TransactionCompleted(cmd.Channel, string(res.Transaction.Kind), string(res.Transaction.Status))
	}
	s.log.InfoContext(ctx, "wager transaction handled",
		slog.String("transactionId", res.Transaction.ID.String()),
		slog.String("walletId", res.Transaction.WalletID.String()),
		slog.String("providerId", res.Transaction.ProviderID),
		slog.String("kind", string(res.Transaction.Kind)),
		slog.String("status", string(res.Transaction.Status)),
		slog.String("failureCode", string(res.Transaction.FailureCode)),
		slog.Bool("idempotentReplay", res.Replay),
		slog.String("channel", cmd.Channel))
	return res, nil
}

func (s *WageringService) processOnce(ctx context.Context, req wager.Request, cmd ProcessCommand) (ProcessResult, error) {
	var res ProcessResult
	err := s.uow.Within(ctx, func(ctx context.Context, tx Tx) error {
		if cmd.Inbox != nil {
			existing, err := tx.Inbox().Register(ctx, *cmd.Inbox)
			if err != nil {
				return err
			}
			if existing != nil {
				if existing.PayloadHash != cmd.Inbox.PayloadHash {
					return ErrInboxHashMismatch
				}
				res.DuplicateMessage = true
			}
		}
		r, err := s.apply(ctx, tx, req, cmd.Meta)
		if err != nil {
			return err
		}
		res.Transaction, res.Replay = r.Transaction, r.Replay || res.DuplicateMessage
		if cmd.Inbox != nil && !res.DuplicateMessage {
			id := res.Transaction.ID
			return tx.Inbox().Complete(ctx, cmd.Inbox.Consumer, cmd.Inbox.MessageID, &id, string(res.Transaction.Status), s.clock.Now())
		}
		return nil
	})
	return res, err
}

// lookupExisting implements the idempotency rules:
//   - same key and same hash      -> replay of the persisted result;
//   - same key, different hash    -> IDEMPOTENCY_KEY_CONFLICT;
//   - same external id, other key -> EXTERNAL_TRANSACTION_CONFLICT.
//
// Both identities are read by a single statement: two separate reads under READ
// COMMITTED could straddle a concurrent commit and misreport a replay as an
// external-id conflict.
func lookupExisting(ctx context.Context, tx Tx, req wager.Request) (*wager.Transaction, error) {
	found, err := tx.Transactions().FindByKeyOrExternalID(ctx, req.ProviderID, req.IdempotencyKey, req.ExternalTransactionID)
	if err != nil {
		return nil, err
	}
	for _, existing := range found {
		if existing.IdempotencyKey() != req.IdempotencyKey {
			continue
		}
		if existing.PayloadHash() != req.PayloadHash() {
			return nil, derr.Wrapf(wager.ErrIdempotencyConflict, "key %q already used with a different payload", req.IdempotencyKey)
		}
		return existing, nil
	}
	if len(found) > 0 {
		return nil, derr.Wrapf(wager.ErrExternalIDConflict, "externalTransactionId %q already recorded with another idempotency key", req.ExternalTransactionID)
	}
	return nil, nil
}

func (s *WageringService) apply(ctx context.Context, tx Tx, req wager.Request, meta events.Meta) (ProcessResult, error) {
	// Fast path: replays do not need the wallet lock.
	existing, err := lookupExisting(ctx, tx, req)
	if err != nil {
		return ProcessResult{}, err
	}
	if existing != nil {
		return ProcessResult{Transaction: existing.Snapshot(), Replay: true}, nil
	}

	// Per-wallet serialization point: every balance change of this wallet waits here.
	w, err := tx.Wallets().GetForUpdate(ctx, req.WalletID)
	if err != nil {
		return ProcessResult{}, err
	}
	t, err := wager.NewExternal(s.ids.New(), req, meta, s.clock.Now())
	if err != nil {
		return ProcessResult{}, err
	}
	inserted, err := tx.Transactions().InsertIfAbsent(ctx, t)
	if err != nil {
		return ProcessResult{}, err
	}
	if !inserted {
		// A concurrent delivery committed between our lookup and the insert.
		existing, err := lookupExisting(ctx, tx, req)
		if err != nil {
			return ProcessResult{}, err
		}
		if existing == nil {
			return ProcessResult{}, fmt.Errorf("%w: idempotency row vanished", ErrConcurrencyConflict)
		}
		return ProcessResult{Transaction: existing.Snapshot(), Replay: true}, nil
	}
	if err := s.execute(ctx, tx, t, w); err != nil {
		return ProcessResult{}, err
	}
	return ProcessResult{Transaction: t.Snapshot()}, nil
}

// execute resolves the reference, applies the domain rules and persists every
// consequence (balance, ledger, transaction state, outbox) in the caller's
// SQL transaction.
func (s *WageringService) execute(ctx context.Context, tx Tx, t *wager.Transaction, w *wallet.Wallet) error {
	var ref wager.ReferenceState
	if t.NeedsReference() {
		r, err := tx.Transactions().FindByExternalID(ctx, t.ProviderID(), t.ReferenceExternalID())
		if err != nil {
			return err
		}
		ref.Tx = r
		if r != nil {
			if ref.AlreadyReversed, err = tx.Transactions().HasProcessedReversal(ctx, r.ID()); err != nil {
				return err
			}
		}
	}
	now := s.clock.Now()
	res, err := t.Execute(wager.ExecuteParams{
		Wallet: w, Reference: ref, LedgerEntryID: s.ids.New(), Schedule: s.cfg.Schedule, Now: now,
	})
	if err != nil {
		return err
	}
	if res.Entry != nil {
		if err := tx.Ledger().Append(ctx, *res.Entry); err != nil {
			return err
		}
		if err := tx.Wallets().UpdateBalance(ctx, w); err != nil {
			return err
		}
	}
	if err := tx.Transactions().Update(ctx, t); err != nil {
		return err
	}
	if t.Status() == wager.StatusProcessed {
		// Operations waiting for this one are retried right away.
		if err := tx.Transactions().WakeWaiting(ctx, t.ProviderID(), t.ExternalTransactionID(), now); err != nil {
			return err
		}
	}
	if t.Status() == wager.StatusRejected && (t.FailureCode() == wager.FailureReferenceNotFound || t.FailureCode() == wager.FailureReferenceNotProcessed) {
		s.metrics.PendingReferenceExpired()
	}
	return tx.Outbox().Append(ctx, t.PullEvents()...)
}

// ResumePending retries due PENDING / PENDING_REFERENCE transactions. It is
// safe to run on every instance: each item is processed under the wallet lock
// (SKIP LOCKED) and re-checked before acting. It returns how many were handled.
func (s *WageringService) ResumePending(ctx context.Context, limit int) (int, error) {
	var due []PendingRef
	err := s.uow.Within(ctx, func(ctx context.Context, tx Tx) error {
		var err error
		due, err = tx.Transactions().DuePending(ctx, s.clock.Now(), limit)
		return err
	})
	if err != nil {
		return 0, err
	}
	handled := 0
	for _, p := range due {
		if ctx.Err() != nil {
			return handled, ctx.Err()
		}
		done, err := s.resumeOne(ctx, p)
		if err != nil {
			if IsRetryable(err) {
				s.metrics.Retry("pending-reference")
				s.log.WarnContext(ctx, "pending transaction retry deferred", slog.String("transactionId", p.TransactionID.String()), slog.Any("error", err))
				continue
			}
			s.markFailed(ctx, p, err)
			continue
		}
		if done {
			handled++
		}
	}
	return handled, nil
}

func (s *WageringService) resumeOne(ctx context.Context, p PendingRef) (bool, error) {
	done := false
	err := s.uow.Within(ctx, func(ctx context.Context, tx Tx) error {
		w, ok, err := tx.Wallets().TryGetForUpdate(ctx, p.WalletID)
		if err != nil || !ok {
			return err // busy wallet: another instance or request holds it; retry later
		}
		t, err := tx.Transactions().GetForUpdate(ctx, p.TransactionID)
		if err != nil {
			return err
		}
		now := s.clock.Now()
		if t.Status().Terminal() || (t.NextAttemptAt() != nil && t.NextAttemptAt().After(now)) {
			return nil // already handled by another instance
		}
		s.metrics.Retry("pending-reference")
		if err := s.execute(ctx, tx, t, w); err != nil {
			return err
		}
		done = true
		s.log.InfoContext(ctx, "pending transaction resumed",
			slog.String("transactionId", t.ID().String()), slog.String("walletId", t.WalletID().String()),
			slog.String("providerId", t.ProviderID()), slog.String("status", string(t.Status())),
			slog.String("failureCode", string(t.FailureCode())), slog.String("correlationId", t.CorrelationID()))
		if t.Status().Terminal() {
			s.metrics.TransactionCompleted(ChannelWorker, string(t.Kind()), string(t.Status()))
		}
		return nil
	})
	return done, err
}

// markFailed records a permanent, non-business failure for audit (FAILED).
func (s *WageringService) markFailed(ctx context.Context, p PendingRef, cause error) {
	s.log.ErrorContext(ctx, "pending transaction failed permanently", slog.String("transactionId", p.TransactionID.String()), slog.Any("error", cause))
	err := s.uow.Within(ctx, func(ctx context.Context, tx Tx) error {
		t, err := tx.Transactions().GetForUpdate(ctx, p.TransactionID)
		if err != nil {
			return err
		}
		if t.Status().Terminal() {
			return nil
		}
		if err := t.MarkFailed(wager.FailurePermanentProcessingError, s.clock.Now()); err != nil {
			return err
		}
		return tx.Transactions().Update(ctx, t)
	})
	if err != nil {
		s.log.ErrorContext(ctx, "could not record FAILED status", slog.String("transactionId", p.TransactionID.String()), slog.Any("error", err))
		return
	}
	s.metrics.TransactionCompleted(ChannelWorker, "", string(wager.StatusFailed))
}

// GetTransaction returns a transaction by internal id.
func (s *WageringService) GetTransaction(ctx context.Context, id uuid.UUID) (wager.Snapshot, error) {
	var snap wager.Snapshot
	err := s.uow.Within(ctx, func(ctx context.Context, tx Tx) error {
		t, err := tx.Transactions().Get(ctx, id)
		if err != nil {
			return err
		}
		snap = t.Snapshot()
		return nil
	})
	return snap, err
}

// GetByExternalID returns a provider transaction by its external id.
func (s *WageringService) GetByExternalID(ctx context.Context, providerID, externalID string) (wager.Snapshot, error) {
	var snap wager.Snapshot
	err := s.uow.Within(ctx, func(ctx context.Context, tx Tx) error {
		t, err := tx.Transactions().FindByExternalID(ctx, providerID, externalID)
		if err != nil {
			return err
		}
		if t == nil {
			return wager.ErrTransactionNotFound
		}
		snap = t.Snapshot()
		return nil
	})
	return snap, err
}
