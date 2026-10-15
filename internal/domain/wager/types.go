// Package wager models WagerTransaction: external provider operations
// (BET, WIN, LOSS, REFUND, ROLLBACK) and the internal OPENING credit.
package wager

import "github.com/julioCAlmeida/backend-challenge-go/internal/domain/derr"

// Kind of a wager transaction.
type Kind string

const (
	KindOpening  Kind = "OPENING"
	KindBet      Kind = "BET"
	KindWin      Kind = "WIN"
	KindLoss     Kind = "LOSS"
	KindRefund   Kind = "REFUND"
	KindRollback Kind = "ROLLBACK"
)

// Status of a wager transaction. See ARCHITECTURE.md for the state machine.
type Status string

const (
	StatusPending          Status = "PENDING"
	StatusPendingReference Status = "PENDING_REFERENCE"
	StatusProcessed        Status = "PROCESSED"
	StatusRejected         Status = "REJECTED"
	StatusFailed           Status = "FAILED"
)

// Terminal reports whether no further transition is allowed.
func (s Status) Terminal() bool {
	return s == StatusProcessed || s == StatusRejected || s == StatusFailed
}

func (s Status) Valid() bool {
	switch s {
	case StatusPending, StatusPendingReference, StatusProcessed, StatusRejected, StatusFailed:
		return true
	}
	return false
}

// Origin distinguishes provider operations from internal ones.
type Origin string

const (
	OriginExternal Origin = "EXTERNAL"
	OriginInternal Origin = "INTERNAL"
)

// FailureCode is the stable, documented reason of a REJECTED or FAILED transaction.
type FailureCode string

const (
	// Definitive business outcomes (retrying the same operation never succeeds).
	FailureInsufficientFunds         FailureCode = "INSUFFICIENT_FUNDS"
	FailureReversalInsufficientFunds FailureCode = "REVERSAL_INSUFFICIENT_FUNDS"
	FailureReferenceAlreadyReversed  FailureCode = "REFERENCE_ALREADY_REVERSED"
	FailureReferenceNotProcessed     FailureCode = "REFERENCE_NOT_PROCESSED"
	FailureReferenceNotFound         FailureCode = "REFERENCE_NOT_FOUND"
	// Correctable inputs: the provider sent data inconsistent with the wallet or the
	// reference. The transaction is terminal; the provider must send a new
	// operation (new externalTransactionId) with corrected data.
	FailureWalletPlayerMismatch    FailureCode = "WALLET_PLAYER_MISMATCH"
	FailureWalletCurrencyMismatch  FailureCode = "WALLET_CURRENCY_MISMATCH"
	FailureReferenceMismatch       FailureCode = "REFERENCE_MISMATCH"
	FailureReferenceAmountMismatch FailureCode = "REFERENCE_AMOUNT_MISMATCH"
	FailureReferenceKindInvalid    FailureCode = "REFERENCE_KIND_INVALID"
	// FAILED: permanent infrastructure failure recorded for audit.
	FailurePermanentProcessingError FailureCode = "PERMANENT_PROCESSING_ERROR"
)

// Correctable reports whether the code describes a correctable input (as opposed
// to a definitive business result).
func (c FailureCode) Correctable() bool {
	switch c {
	case FailureWalletPlayerMismatch, FailureWalletCurrencyMismatch, FailureReferenceMismatch,
		FailureReferenceAmountMismatch, FailureReferenceKindInvalid:
		return true
	}
	return false
}

var (
	ErrInvalidKind         = derr.New(derr.KindValidation, "INVALID_KIND", "kind must be one of BET, WIN, LOSS, REFUND, ROLLBACK")
	ErrKindNotAllowed      = derr.New(derr.KindValidation, "KIND_NOT_ALLOWED", "OPENING is reserved for internal wallet opening")
	ErrInvalidRequest      = derr.New(derr.KindValidation, "INVALID_REQUEST", "invalid wager request")
	ErrInvalidAmount       = derr.New(derr.KindValidation, "INVALID_AMOUNT_FOR_KIND", "amount is not allowed for this kind")
	ErrReferenceRequired   = derr.New(derr.KindValidation, "REFERENCE_REQUIRED", "referenceExternalTransactionId is required for REFUND and ROLLBACK")
	ErrReferenceNotAllowed = derr.New(derr.KindValidation, "REFERENCE_NOT_ALLOWED", "referenceExternalTransactionId is not allowed for this kind")
	ErrInvalidTransition   = derr.New(derr.KindInvariant, "INVALID_TRANSITION", "invalid status transition")
	ErrInvalidTransaction  = derr.New(derr.KindInvariant, "INVALID_TRANSACTION", "invalid transaction data")

	ErrIdempotencyConflict = derr.New(derr.KindConflict, "IDEMPOTENCY_KEY_CONFLICT", "idempotency key was already used with a different payload")
	ErrExternalIDConflict  = derr.New(derr.KindConflict, "EXTERNAL_TRANSACTION_CONFLICT", "externalTransactionId was already used with another idempotency key or payload")
	ErrTransactionNotFound = derr.New(derr.KindNotFound, "TRANSACTION_NOT_FOUND", "transaction not found")
)

// ParseExternalKind accepts only the kinds a provider may send.
func ParseExternalKind(s string) (Kind, error) {
	switch Kind(s) {
	case KindBet, KindWin, KindLoss, KindRefund, KindRollback:
		return Kind(s), nil
	case KindOpening:
		return "", ErrKindNotAllowed
	}
	return "", derr.Wrapf(ErrInvalidKind, "unknown kind %q", s)
}

// ParseKind accepts every kind, OPENING included; used for rehydration.
func ParseKind(s string) (Kind, error) {
	if Kind(s) == KindOpening {
		return KindOpening, nil
	}
	return ParseExternalKind(s)
}
