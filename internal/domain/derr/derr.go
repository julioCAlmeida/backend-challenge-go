// Package derr defines the classifiable error model shared by the domain packages.
//
// Every business rejection is an *Error carrying a stable Code and a Kind. Callers
// classify errors with errors.Is (against a sentinel with the same code) or with
// errors.As / KindOf. The domain never panics to signal a business rejection.
package derr

import (
	"errors"
	"fmt"
)

// Kind groups error codes by how callers must react to them.
type Kind string

const (
	// KindValidation means the input is malformed or violates a value-object rule.
	// It is correctable by the caller and nothing is persisted.
	KindValidation Kind = "VALIDATION"
	// KindConflict means the request conflicts with persisted state
	// (idempotency key reused with a different payload, duplicated wallet...).
	KindConflict Kind = "CONFLICT"
	// KindNotFound means a referenced aggregate does not exist.
	KindNotFound Kind = "NOT_FOUND"
	// KindRejected means a business rule definitively refused the operation.
	KindRejected Kind = "REJECTED"
	// KindInvariant means an internal invariant would be broken. It indicates a bug
	// or corrupted state, never a user error.
	KindInvariant Kind = "INVARIANT"
)

// Error is a classifiable domain error.
type Error struct {
	Kind    Kind
	Code    string
	Message string
}

func (e *Error) Error() string {
	if e.Message == "" {
		return e.Code
	}
	return e.Code + ": " + e.Message
}

// Is matches any *Error with the same code, so sentinels work with errors.Is even
// when the returned error carries a more specific message.
func (e *Error) Is(target error) bool {
	var t *Error
	if !errors.As(target, &t) {
		return false
	}
	return t.Code == e.Code
}

// New builds a sentinel error.
func New(kind Kind, code, message string) *Error {
	return &Error{Kind: kind, Code: code, Message: message}
}

// Wrapf returns a copy of the sentinel with a more specific message.
func Wrapf(sentinel *Error, format string, args ...any) *Error {
	return &Error{Kind: sentinel.Kind, Code: sentinel.Code, Message: fmt.Sprintf(format, args...)}
}

// KindOf returns the kind of a domain error, or "" when err is not a domain error.
func KindOf(err error) Kind {
	var e *Error
	if errors.As(err, &e) {
		return e.Kind
	}
	return ""
}

// CodeOf returns the code of a domain error, or "" when err is not a domain error.
func CodeOf(err error) string {
	var e *Error
	if errors.As(err, &e) {
		return e.Code
	}
	return ""
}
