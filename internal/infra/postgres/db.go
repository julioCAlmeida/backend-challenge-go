// Package postgres implements the persistence ports with pgx and explicit SQL.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/julioCAlmeida/backend-challenge-go/internal/app"
)

// Config of the connection pool.
type Config struct {
	URL              string
	MaxConns         int32
	StatementTimeout time.Duration
	LockTimeout      time.Duration
}

// NewPool builds the pool. Connections are established lazily; call Ping (done
// by the Fx lifecycle) to validate the dependency at startup.
func NewPool(ctx context.Context, cfg Config) (*pgxpool.Pool, error) {
	pc, err := pgxpool.ParseConfig(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	if cfg.MaxConns > 0 {
		pc.MaxConns = cfg.MaxConns
	}
	pc.ConnConfig.ConnectTimeout = 5 * time.Second
	if pc.ConnConfig.RuntimeParams == nil {
		pc.ConnConfig.RuntimeParams = map[string]string{}
	}
	if cfg.StatementTimeout > 0 {
		pc.ConnConfig.RuntimeParams["statement_timeout"] = fmt.Sprint(cfg.StatementTimeout.Milliseconds())
	}
	if cfg.LockTimeout > 0 {
		pc.ConnConfig.RuntimeParams["lock_timeout"] = fmt.Sprint(cfg.LockTimeout.Milliseconds())
	}
	pc.ConnConfig.RuntimeParams["application_name"] = "wallet-service"
	return pgxpool.NewWithConfig(ctx, pc)
}

// PingWithRetry waits until PostgreSQL answers or ctx expires.
func PingWithRetry(ctx context.Context, pool *pgxpool.Pool, log *slog.Logger) error {
	backoff := 200 * time.Millisecond
	for {
		pctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		err := pool.Ping(pctx)
		cancel()
		if err == nil {
			return nil
		}
		log.WarnContext(ctx, "postgres not ready", slog.Any("error", err))
		select {
		case <-ctx.Done():
			return fmt.Errorf("postgres unavailable: %w", errors.Join(err, ctx.Err()))
		case <-time.After(backoff):
		}
		if backoff < 2*time.Second {
			backoff *= 2
		}
	}
}

// Classify maps driver errors to the app error classes. Errors that are not
// infrastructure failures (domain errors, constraint violations) pass through.
func Classify(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, app.ErrTransient) || errors.Is(err, app.ErrConcurrencyConflict) {
		return err
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch {
		case pgErr.Code == "40001" || pgErr.Code == "40P01":
			return fmt.Errorf("%w: %s", app.ErrConcurrencyConflict, pgErr.Message)
		case pgErr.Code == "55P03", pgErr.Code == "57014", pgErr.Code == "53300", pgErr.Code == "53400",
			pgErr.Code[:2] == "08", pgErr.Code[:2] == "57":
			return fmt.Errorf("%w: postgres %s: %s", app.ErrTransient, pgErr.Code, pgErr.Message)
		}
		return err
	}
	var connectErr *pgconn.ConnectError
	var netErr net.Error
	if errors.As(err, &connectErr) || errors.As(err, &netErr) || pgconn.Timeout(err) || pgconn.SafeToRetry(err) ||
		errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) ||
		errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return fmt.Errorf("%w: %w", app.ErrTransient, err)
	}
	return err
}

func isUniqueViolation(err error, constraint string) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505" && (constraint == "" || pgErr.ConstraintName == constraint)
}

// querier is satisfied by pgx.Tx and *pgxpool.Pool.
type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}
