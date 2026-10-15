// Package observability provides JSON logging with request-scoped identifiers
// and the Prometheus metrics of the service.
package observability

import (
	"context"
	"io"
	"log/slog"
	"strings"
)

type ctxKey struct{}

// WithLogAttrs returns a context whose log records carry attrs (correlationId,
// messageId, providerId, ...). Later values for the same key win.
func WithLogAttrs(ctx context.Context, attrs ...slog.Attr) context.Context {
	prev, _ := ctx.Value(ctxKey{}).([]slog.Attr)
	merged := make([]slog.Attr, 0, len(prev)+len(attrs))
	merged = append(merged, prev...)
	merged = append(merged, attrs...)
	return context.WithValue(ctx, ctxKey{}, merged)
}

// LogAttr returns the value stored for key in ctx, or "".
func LogAttr(ctx context.Context, key string) string {
	attrs, _ := ctx.Value(ctxKey{}).([]slog.Attr)
	for i := len(attrs) - 1; i >= 0; i-- {
		if attrs[i].Key == key {
			return attrs[i].Value.String()
		}
	}
	return ""
}

type contextHandler struct{ slog.Handler }

func (h contextHandler) Handle(ctx context.Context, r slog.Record) error {
	if attrs, ok := ctx.Value(ctxKey{}).([]slog.Attr); ok {
		seen := map[string]bool{}
		r.Attrs(func(a slog.Attr) bool { seen[a.Key] = true; return true })
		for i := len(attrs) - 1; i >= 0; i-- {
			if !seen[attrs[i].Key] {
				seen[attrs[i].Key] = true
				r.AddAttrs(attrs[i])
			}
		}
	}
	return h.Handler.Handle(ctx, r)
}

func (h contextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return contextHandler{h.Handler.WithAttrs(attrs)}
}

func (h contextHandler) WithGroup(name string) slog.Handler {
	return contextHandler{h.Handler.WithGroup(name)}
}

// NewLogger builds the JSON logger.
func NewLogger(w io.Writer, level, instanceID string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	h := slog.NewJSONHandler(w, &slog.HandlerOptions{Level: lvl})
	return slog.New(contextHandler{h}).With(slog.String("service", "wallet-service"), slog.String("instance", instanceID))
}
