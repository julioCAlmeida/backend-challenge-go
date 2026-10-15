package httpapi

import (
	"errors"
	"log/slog"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/julioCAlmeida/backend-challenge-go/internal/auth"
	"github.com/julioCAlmeida/backend-challenge-go/internal/observability"
)

const headerCorrelationID = "X-Correlation-Id"

var correlationPattern = regexp.MustCompile(`^[A-Za-z0-9._:\-]{1,128}$`)

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

// observe adds the correlation id, structured access logs, metrics and panic recovery.
func (a *API) observe(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		correlationID := r.Header.Get(headerCorrelationID)
		if !correlationPattern.MatchString(correlationID) {
			correlationID = uuid.NewString()
		}
		w.Header().Set(headerCorrelationID, correlationID)
		ctx := observability.WithLogAttrs(r.Context(), slog.String("correlationId", correlationID))
		r = r.WithContext(ctx)
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		defer func() {
			if p := recover(); p != nil {
				a.log.ErrorContext(ctx, "panic in handler", slog.Any("panic", p))
				writeError(rec, r, http.StatusInternalServerError, "INTERNAL_ERROR", "unexpected error")
			}
			route := r.Pattern
			if route == "" {
				route = "unmatched"
			}
			a.metrics.HTTPRequests.WithLabelValues(route, strconv.Itoa(rec.status)).Inc()
			a.metrics.HTTPDuration.WithLabelValues(route).Observe(time.Since(start).Seconds())
			if !strings.HasPrefix(route, "GET /health") && route != "GET /metrics" {
				a.log.InfoContext(r.Context(), "http request", slog.String("method", r.Method), slog.String("route", route),
					slog.Int("status", rec.status), slog.Duration("duration", time.Since(start)))
			}
		}()
		next.ServeHTTP(rec, r)
	})
}

// authenticated validates the bearer token and enforces allow(principal).
// Authorization failures are answered before any handler logic runs, so they
// never have financial effects.
func (a *API) authenticated(allow func(auth.Principal) bool, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		raw, ok := bearer(r)
		if !ok {
			a.metrics.AuthFailures.WithLabelValues("missing").Inc()
			w.Header().Set("WWW-Authenticate", `Bearer realm="wagering"`)
			writeError(w, r, http.StatusUnauthorized, "UNAUTHENTICATED", "missing bearer token")
			return
		}
		p, err := a.verifier.Verify(r.Context(), raw)
		if err != nil {
			reason, code := "invalid", "INVALID_TOKEN"
			if errors.Is(err, auth.ErrExpiredToken) {
				reason, code = "expired", "TOKEN_EXPIRED"
			}
			a.metrics.AuthFailures.WithLabelValues(reason).Inc()
			a.log.InfoContext(r.Context(), "authentication rejected", slog.String("reason", reason), slog.Any("error", err))
			w.Header().Set("WWW-Authenticate", `Bearer realm="wagering", error="invalid_token"`)
			writeError(w, r, http.StatusUnauthorized, code, "the access token is invalid or expired")
			return
		}
		if !allow(p) {
			a.metrics.AuthFailures.WithLabelValues("forbidden").Inc()
			writeError(w, r, http.StatusForbidden, "FORBIDDEN", "the authenticated client is not allowed to perform this operation")
			return
		}
		ctx := auth.WithPrincipal(r.Context(), p)
		ctx = observability.WithLogAttrs(ctx, slog.String("clientId", p.ClientID))
		if p.ProviderID != "" {
			ctx = observability.WithLogAttrs(ctx, slog.String("providerId", p.ProviderID))
		}
		next(w, r.WithContext(ctx))
	}
}

func bearer(r *http.Request) (string, bool) {
	h := r.Header.Get("Authorization")
	const prefix = "bearer "
	if len(h) <= len(prefix) || !strings.EqualFold(h[:len(prefix)], prefix) {
		return "", false
	}
	return strings.TrimSpace(h[len(prefix):]), true
}

func providerOnly(p auth.Principal) bool       { return p.IsProvider() }
func operatorOnly(p auth.Principal) bool       { return p.IsOperator() }
func providerOrOperator(p auth.Principal) bool { return p.IsProvider() || p.IsOperator() }
