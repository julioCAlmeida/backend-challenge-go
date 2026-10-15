// Package config loads and validates the service configuration from the environment.
package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	InstanceID      string
	HTTPAddr        string
	LogLevel        string
	ShutdownTimeout time.Duration
	StartTimeout    time.Duration

	DatabaseURL              string
	DatabaseMaxConns         int32
	DatabaseStatementTimeout time.Duration
	DatabaseLockTimeout      time.Duration
	ConflictRetries          int

	AWSRegion          string
	SQSEndpoint        string
	AWSAccessKeyID     string
	AWSSecretAccessKey string
	WagerQueue         string
	WagerDLQ           string
	EventsQueue        string

	ConsumerEnabled           bool
	ConsumerName              string
	ConsumerWorkers           int
	ConsumerWaitTime          time.Duration
	ConsumerVisibilityTimeout time.Duration
	ConsumerProcessingTimeout time.Duration
	ConsumerMaxReceiveCount   int
	ConsumerBackoffBase       time.Duration
	ConsumerBackoffMax        time.Duration

	OutboxEnabled      bool
	OutboxWorkers      int
	OutboxPollInterval time.Duration
	OutboxBatchSize    int
	OutboxLease        time.Duration
	OutboxBackoffBase  time.Duration
	OutboxBackoffMax   time.Duration

	PendingEnabled      bool
	PendingPollInterval time.Duration
	PendingBatchSize    int
	PendingBackoffBase  time.Duration
	PendingBackoffMax   time.Duration
	PendingMaxAttempts  int
	PendingTTL          time.Duration

	OIDCIssuerURL string
	OIDCJWKSURL   string
	OIDCAudience  string

	// FaultInjection enables crash points for failure-recovery tests only.
	FaultInjection []string
}

// Load reads the environment and validates the result.
func Load() (Config, error) {
	return LoadFrom(os.Getenv)
}

// LoadFrom reads configuration through getenv (used by tests).
func LoadFrom(getenv func(string) string) (Config, error) {
	r := reader{getenv: getenv}
	host, _ := os.Hostname()
	c := Config{
		InstanceID:      r.str("INSTANCE_ID", host),
		HTTPAddr:        r.str("HTTP_ADDR", ":8080"),
		LogLevel:        r.str("LOG_LEVEL", "info"),
		ShutdownTimeout: r.dur("SHUTDOWN_TIMEOUT", 25*time.Second),
		StartTimeout:    r.dur("START_TIMEOUT", 60*time.Second),

		DatabaseURL:              r.str("DATABASE_URL", ""),
		DatabaseMaxConns:         int32(r.int("DATABASE_MAX_CONNS", 20)),
		DatabaseStatementTimeout: r.dur("DATABASE_STATEMENT_TIMEOUT", 10*time.Second),
		DatabaseLockTimeout:      r.dur("DATABASE_LOCK_TIMEOUT", 5*time.Second),
		ConflictRetries:          r.int("CONFLICT_RETRIES", 5),

		AWSRegion:          r.str("AWS_REGION", "us-east-1"),
		SQSEndpoint:        r.str("SQS_ENDPOINT", ""),
		AWSAccessKeyID:     r.str("AWS_ACCESS_KEY_ID", ""),
		AWSSecretAccessKey: r.str("AWS_SECRET_ACCESS_KEY", ""),
		WagerQueue:         r.str("SQS_WAGER_QUEUE", "wager-transactions.fifo"),
		WagerDLQ:           r.str("SQS_WAGER_DLQ", "wager-transactions-dlq.fifo"),
		EventsQueue:        r.str("SQS_EVENTS_QUEUE", "wallet-events.fifo"),

		ConsumerEnabled:           r.bool("SQS_CONSUMER_ENABLED", true),
		ConsumerName:              r.str("SQS_CONSUMER_NAME", "wager-transactions-consumer"),
		ConsumerWorkers:           r.int("SQS_CONSUMER_WORKERS", 2),
		ConsumerWaitTime:          r.dur("SQS_WAIT_TIME", 10*time.Second),
		ConsumerVisibilityTimeout: r.dur("SQS_VISIBILITY_TIMEOUT", 30*time.Second),
		ConsumerProcessingTimeout: r.dur("SQS_PROCESSING_TIMEOUT", 20*time.Second),
		ConsumerMaxReceiveCount:   r.int("SQS_MAX_RECEIVE_COUNT", 5),
		ConsumerBackoffBase:       r.dur("SQS_BACKOFF_BASE", 2*time.Second),
		ConsumerBackoffMax:        r.dur("SQS_BACKOFF_MAX", 5*time.Minute),

		OutboxEnabled:      r.bool("OUTBOX_ENABLED", true),
		OutboxWorkers:      r.int("OUTBOX_WORKERS", 4),
		OutboxPollInterval: r.dur("OUTBOX_POLL_INTERVAL", 500*time.Millisecond),
		OutboxBatchSize:    r.int("OUTBOX_BATCH_SIZE", 50),
		OutboxLease:        r.dur("OUTBOX_LEASE", 30*time.Second),
		OutboxBackoffBase:  r.dur("OUTBOX_BACKOFF_BASE", time.Second),
		OutboxBackoffMax:   r.dur("OUTBOX_BACKOFF_MAX", 5*time.Minute),

		PendingEnabled:      r.bool("PENDING_ENABLED", true),
		PendingPollInterval: r.dur("PENDING_POLL_INTERVAL", time.Second),
		PendingBatchSize:    r.int("PENDING_BATCH_SIZE", 50),
		PendingBackoffBase:  r.dur("PENDING_BACKOFF_BASE", 2*time.Second),
		PendingBackoffMax:   r.dur("PENDING_BACKOFF_MAX", time.Minute),
		PendingMaxAttempts:  r.int("PENDING_MAX_ATTEMPTS", 10),
		PendingTTL:          r.dur("PENDING_TTL", 15*time.Minute),

		OIDCIssuerURL: r.str("OIDC_ISSUER_URL", ""),
		OIDCJWKSURL:   r.str("OIDC_JWKS_URL", ""),
		OIDCAudience:  r.str("OIDC_AUDIENCE", "wagering-api"),
	}
	if fi := r.str("FAULT_INJECTION", ""); fi != "" {
		c.FaultInjection = strings.Split(fi, ",")
	}
	if c.OIDCJWKSURL == "" && c.OIDCIssuerURL != "" {
		c.OIDCJWKSURL = strings.TrimRight(c.OIDCIssuerURL, "/") + "/protocol/openid-connect/certs"
	}
	if len(r.errs) > 0 {
		return Config{}, errors.Join(r.errs...)
	}
	return c, c.Validate()
}

// Validate rejects inconsistent configuration at startup.
func (c Config) Validate() error {
	var errs []error
	req := func(name, v string) {
		if v == "" {
			errs = append(errs, fmt.Errorf("%s is required", name))
		}
	}
	req("DATABASE_URL", c.DatabaseURL)
	req("OIDC_ISSUER_URL", c.OIDCIssuerURL)
	req("OIDC_AUDIENCE", c.OIDCAudience)
	req("SQS_WAGER_QUEUE", c.WagerQueue)
	req("SQS_WAGER_DLQ", c.WagerDLQ)
	req("SQS_EVENTS_QUEUE", c.EventsQueue)
	req("INSTANCE_ID", c.InstanceID)
	for name, u := range map[string]string{"OIDC_ISSUER_URL": c.OIDCIssuerURL, "OIDC_JWKS_URL": c.OIDCJWKSURL} {
		if u == "" {
			continue
		}
		if p, err := url.Parse(u); err != nil || p.Scheme == "" || p.Host == "" {
			errs = append(errs, fmt.Errorf("%s must be an absolute URL", name))
		}
	}
	positive := map[string]time.Duration{
		"SHUTDOWN_TIMEOUT": c.ShutdownTimeout, "SQS_VISIBILITY_TIMEOUT": c.ConsumerVisibilityTimeout,
		"SQS_PROCESSING_TIMEOUT": c.ConsumerProcessingTimeout, "OUTBOX_POLL_INTERVAL": c.OutboxPollInterval,
		"OUTBOX_LEASE": c.OutboxLease, "PENDING_POLL_INTERVAL": c.PendingPollInterval,
		"PENDING_BACKOFF_BASE": c.PendingBackoffBase, "OUTBOX_BACKOFF_BASE": c.OutboxBackoffBase,
	}
	for name, d := range positive {
		if d <= 0 {
			errs = append(errs, fmt.Errorf("%s must be positive", name))
		}
	}
	if c.ConsumerProcessingTimeout >= c.ConsumerVisibilityTimeout {
		errs = append(errs, errors.New("SQS_PROCESSING_TIMEOUT must be lower than SQS_VISIBILITY_TIMEOUT"))
	}
	if c.ConsumerWaitTime < 0 || c.ConsumerWaitTime > 20*time.Second {
		errs = append(errs, errors.New("SQS_WAIT_TIME must be between 0 and 20s"))
	}
	if c.ConsumerWorkers < 1 || c.OutboxWorkers < 1 || c.OutboxBatchSize < 1 || c.PendingBatchSize < 1 || c.PendingMaxAttempts < 1 || c.ConflictRetries < 1 {
		errs = append(errs, errors.New("worker counts, batch sizes, attempts and retries must be >= 1"))
	}
	if c.DatabaseMaxConns < 2 {
		errs = append(errs, errors.New("DATABASE_MAX_CONNS must be >= 2"))
	}
	return errors.Join(errs...)
}

type reader struct {
	getenv func(string) string
	errs   []error
}

func (r *reader) str(k, def string) string {
	if v := strings.TrimSpace(r.getenv(k)); v != "" {
		return v
	}
	return def
}

func (r *reader) int(k string, def int) int {
	v := r.getenv(k)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		r.errs = append(r.errs, fmt.Errorf("%s: invalid integer %q", k, v))
	}
	return n
}

func (r *reader) bool(k string, def bool) bool {
	v := r.getenv(k)
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		r.errs = append(r.errs, fmt.Errorf("%s: invalid boolean %q", k, v))
	}
	return b
}

func (r *reader) dur(k string, def time.Duration) time.Duration {
	v := r.getenv(k)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		r.errs = append(r.errs, fmt.Errorf("%s: invalid duration %q", k, v))
	}
	return d
}
