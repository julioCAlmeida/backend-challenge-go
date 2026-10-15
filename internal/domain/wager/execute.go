package wager

import (
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/julioCAlmeida/backend-challenge-go/internal/domain/derr"
	"github.com/julioCAlmeida/backend-challenge-go/internal/domain/events"
	"github.com/julioCAlmeida/backend-challenge-go/internal/domain/money"
	"github.com/julioCAlmeida/backend-challenge-go/internal/domain/wallet"
)

// RetrySchedule decides when a pending reference is retried and when it expires.
type RetrySchedule interface {
	// Next returns the next attempt time after `attempts` failed retries, or
	// expired=true when the maximum number of attempts or the TTL is exhausted.
	Next(attempts int, createdAt, now time.Time) (next time.Time, expired bool)
}

// ReferenceState is what the caller resolved for (providerId, referenceExternalTransactionId).
type ReferenceState struct {
	// Tx is nil when the reference has not arrived yet.
	Tx *Transaction
	// AlreadyReversed is true when a PROCESSED REFUND or ROLLBACK already targets Tx.
	AlreadyReversed bool
}

// ExecuteParams groups the inputs of Execute.
type ExecuteParams struct {
	Wallet        *wallet.Wallet
	Reference     ReferenceState
	LedgerEntryID uuid.UUID
	Schedule      RetrySchedule
	Now           time.Time
}

// Result is what Execute produced. Entry is nil when the balance did not move.
type Result struct {
	Entry *wallet.LedgerEntry
}

// Execute applies the transaction to its (locked) wallet following the rules of
// each kind. It moves the transaction to PROCESSED, REJECTED or PENDING_REFERENCE
// and records the integration events. Business rejections are not errors: the
// returned error is non-nil only for invariant violations.
//
// Rules:
//   - BET debits; insufficient balance -> INSUFFICIENT_FUNDS.
//   - WIN credits; an optional reference must be a BET of the same round.
//   - LOSS has no movement (amount 0.00).
//   - REFUND credits back a processed BET, same amount.
//   - ROLLBACK reverses a processed BET (credit), WIN or REFUND (debit, insufficient
//     balance -> REVERSAL_INSUFFICIENT_FUNDS), same amount.
//   - A transaction receives at most one successful reversal (REFUND or ROLLBACK).
//     Reversing a REFUND with a ROLLBACK is allowed and does not release the BET.
func (t *Transaction) Execute(p ExecuteParams) (Result, error) {
	if t.status.Terminal() {
		return Result{}, derr.Wrapf(ErrInvalidTransition, "transaction %s is %s", t.id, t.status)
	}
	if t.origin != OriginExternal {
		return Result{}, derr.Wrapf(ErrInvalidTransaction, "only external transactions are executed")
	}
	w := p.Wallet
	if w == nil || w.ID() != t.walletID {
		return Result{}, derr.Wrapf(ErrInvalidTransaction, "wallet does not match transaction")
	}
	now := p.Now.UTC()

	if w.PlayerID() != t.playerID {
		return Result{}, t.MarkRejected(FailureWalletPlayerMismatch, nil, now)
	}
	balance := w.Balance()
	if w.Currency() != t.money.Currency() {
		// The wallet balance is in another currency: it is not reported.
		return Result{}, t.MarkRejected(FailureWalletCurrencyMismatch, nil, now)
	}

	var ref *Transaction
	if t.NeedsReference() {
		r, code, wait := t.checkReference(p.Reference)
		if code != "" {
			return Result{}, t.MarkRejected(code, &balance, now)
		}
		if wait {
			return Result{}, t.waitForReference(p.Reference.Tx != nil, p.Schedule, &balance, now)
		}
		ref = r
	}

	var dir wallet.Direction
	switch t.kind {
	case KindLoss:
		return Result{}, t.markProcessed(balance, ref, now)
	case KindBet:
		dir = wallet.Debit
	case KindWin, KindRefund:
		dir = wallet.Credit
	case KindRollback:
		dir = wallet.Debit
		if ref.kind == KindBet {
			dir = wallet.Credit
		}
	default:
		return Result{}, derr.Wrapf(ErrInvalidTransaction, "kind %s cannot be executed", t.kind)
	}

	var entry wallet.LedgerEntry
	var err error
	if dir == wallet.Debit {
		entry, err = w.Debit(t.money, t.id, p.LedgerEntryID, now)
	} else {
		entry, err = w.Credit(t.money, t.id, p.LedgerEntryID, now)
	}
	if errors.Is(err, wallet.ErrInsufficientFunds) {
		code := FailureInsufficientFunds
		if t.kind == KindRollback {
			code = FailureReversalInsufficientFunds
		}
		return Result{}, t.MarkRejected(code, &balance, now)
	}
	if err != nil {
		return Result{}, err
	}
	if err := t.markProcessed(entry.BalanceAfter(), ref, now); err != nil {
		return Result{}, err
	}
	t.recordBalanceChanged(entry, now)
	return Result{Entry: &entry}, nil
}

// checkReference validates the resolved reference. It returns a rejection code,
// or wait=true when the reference is missing or still pending.
func (t *Transaction) checkReference(rs ReferenceState) (*Transaction, FailureCode, bool) {
	ref := rs.Tx
	if ref == nil {
		return nil, "", true
	}
	switch t.kind {
	case KindWin, KindRefund:
		if ref.kind != KindBet {
			return nil, FailureReferenceKindInvalid, false
		}
	case KindRollback:
		if ref.kind != KindBet && ref.kind != KindWin && ref.kind != KindRefund {
			return nil, FailureReferenceKindInvalid, false
		}
	default:
		return nil, FailureReferenceKindInvalid, false
	}
	if ref.providerID != t.providerID || ref.playerID != t.playerID || ref.walletID != t.walletID ||
		ref.money.Currency() != t.money.Currency() || ref.roundID != t.roundID {
		return nil, FailureReferenceMismatch, false
	}
	if (t.kind == KindRefund || t.kind == KindRollback) && !ref.money.Equal(t.money) {
		return nil, FailureReferenceAmountMismatch, false
	}
	switch ref.status {
	case StatusPending, StatusPendingReference:
		return nil, "", true
	case StatusRejected, StatusFailed:
		return nil, FailureReferenceNotProcessed, false
	}
	if (t.kind == KindRefund || t.kind == KindRollback) && rs.AlreadyReversed {
		return nil, FailureReferenceAlreadyReversed, false
	}
	return ref, "", false
}

func (t *Transaction) waitForReference(refExists bool, s RetrySchedule, balance *money.Money, now time.Time) error {
	attempts := t.referenceAttempts
	if t.status == StatusPendingReference {
		attempts++
	}
	next, expired := s.Next(attempts, t.createdAt, now)
	if expired {
		code := FailureReferenceNotFound
		if refExists {
			code = FailureReferenceNotProcessed
		}
		if t.status == StatusPendingReference {
			t.referenceAttempts = attempts
		}
		return t.MarkRejected(code, balance, now)
	}
	return t.MarkPendingReference(next, now)
}

func (t *Transaction) recordBalanceChanged(e wallet.LedgerEntry, now time.Time) {
	t.recorded = append(t.recorded, events.NewWalletBalanceChanged(t.Meta(), now, events.WalletBalanceChangedData{
		WalletID: e.WalletID(), TransactionID: t.id, Direction: string(e.Direction()), Money: e.Amount(),
		BalanceBefore: e.BalanceBefore(), BalanceAfter: e.BalanceAfter(), WalletVersion: e.WalletVersion(),
	}))
}

// CompleteOpening finishes the internal OPENING once the wallet was opened with
// its credit entry: PENDING -> PROCESSED, recording WagerTransactionProcessed and
// WalletBalanceChanged.
func (t *Transaction) CompleteOpening(entry wallet.LedgerEntry, now time.Time) error {
	if t.kind != KindOpening {
		return derr.Wrapf(ErrInvalidTransition, "not an OPENING transaction")
	}
	if entry.TransactionID() != t.id || entry.WalletID() != t.walletID || !entry.Amount().Equal(t.money) || entry.Direction() != wallet.Credit {
		return derr.Wrapf(ErrInvalidTransaction, "opening entry does not match the transaction")
	}
	if err := t.markProcessed(entry.BalanceAfter(), nil, now); err != nil {
		return err
	}
	t.recordBalanceChanged(entry, now)
	return nil
}
