package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/julioCAlmeida/backend-challenge-go/internal/app"
	"github.com/julioCAlmeida/backend-challenge-go/internal/domain/derr"
)

// ErrorBody is the shape of every error response:
//
//	{"error":{"code":"IDEMPOTENCY_KEY_CONFLICT","message":"...","correlationId":"..."}}
type ErrorBody struct {
	Error ErrorDetail `json:"error"`
}

type ErrorDetail struct {
	Code          string `json:"code"`
	Message       string `json:"message"`
	CorrelationID string `json:"correlationId,omitempty"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, r *http.Request, status int, code, msg string) {
	writeJSON(w, status, ErrorBody{Error: ErrorDetail{Code: code, Message: msg, CorrelationID: w.Header().Get(headerCorrelationID)}})
}

// writeAppError maps errors to the HTTP contract:
//
//	400 validation (malformed body, invalid money, unknown kind, OPENING, bad cursor)
//	404 aggregate not found
//	409 conflict (idempotency key/external id reuse, duplicated wallet, message hash)
//	503 transient infrastructure failure or exhausted concurrency retries (Retry-After)
//	500 invariant violation or unexpected error
func writeAppError(w http.ResponseWriter, r *http.Request, log *slog.Logger, err error) {
	var de *derr.Error
	switch {
	case errors.As(err, &de) && de.Kind == derr.KindValidation:
		writeError(w, r, http.StatusBadRequest, de.Code, de.Error())
	case errors.As(err, &de) && de.Kind == derr.KindNotFound:
		writeError(w, r, http.StatusNotFound, de.Code, de.Message)
	case errors.As(err, &de) && de.Kind == derr.KindConflict:
		writeError(w, r, http.StatusConflict, de.Code, de.Error())
	case errors.As(err, &de) && de.Kind == derr.KindRejected:
		writeError(w, r, http.StatusUnprocessableEntity, de.Code, de.Error())
	case app.IsRetryable(err) || errors.Is(err, context.DeadlineExceeded):
		log.WarnContext(r.Context(), "transient failure", slog.Any("error", err))
		w.Header().Set("Retry-After", "1")
		writeError(w, r, http.StatusServiceUnavailable, "TEMPORARILY_UNAVAILABLE", "a dependency is temporarily unavailable; retry with the same Idempotency-Key")
	default:
		log.ErrorContext(r.Context(), "unexpected error", slog.Any("error", err))
		writeError(w, r, http.StatusInternalServerError, "INTERNAL_ERROR", "unexpected error")
	}
}
