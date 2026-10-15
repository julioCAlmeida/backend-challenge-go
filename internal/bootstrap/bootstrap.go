// Package bootstrap composes the application with Uber Fx.
//
// Lifecycle ordering: Fx runs OnStart hooks in registration order and OnStop
// hooks in reverse. Dependencies (PostgreSQL, SQS, IdP) register their hooks
// when constructed, i.e. before the workers and the HTTP server that use them,
// so on shutdown the HTTP server stops accepting requests first, then the
// workers drain, and only then are connections closed.
package bootstrap

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"

	"github.com/julioCAlmeida/backend-challenge-go/internal/app"
	"github.com/julioCAlmeida/backend-challenge-go/internal/auth"
	"github.com/julioCAlmeida/backend-challenge-go/internal/config"
	"github.com/julioCAlmeida/backend-challenge-go/internal/faults"
	"github.com/julioCAlmeida/backend-challenge-go/internal/httpapi"
	"github.com/julioCAlmeida/backend-challenge-go/internal/infra/postgres"
	"github.com/julioCAlmeida/backend-challenge-go/internal/infra/sqsx"
	"github.com/julioCAlmeida/backend-challenge-go/internal/observability"
	"github.com/julioCAlmeida/backend-challenge-go/internal/worker"
)

// Module is the whole application for a validated configuration.
func Module(cfg config.Config) fx.Option {
	return fx.Options(
		fx.Supply(cfg),
		ObservabilityModule,
		fx.WithLogger(func(l *slog.Logger) fxevent.Logger {
			return &fxevent.SlogLogger{Logger: l.With(slog.String("component", "fx"))}
		}),
		PostgresModule,
		SQSModule,
		AuthModule,
		AppModule,
		WorkersModule,
		HTTPModule,
	)
}

// New builds the Fx application with the configured timeouts.
func New(cfg config.Config, extra ...fx.Option) *fx.App {
	opts := []fx.Option{Module(cfg), fx.StartTimeout(cfg.StartTimeout), fx.StopTimeout(cfg.ShutdownTimeout)}
	return fx.New(append(opts, extra...)...)
}

var ObservabilityModule = fx.Module("observability",
	fx.Provide(
		func(cfg config.Config) *slog.Logger {
			return observability.NewLogger(os.Stdout, cfg.LogLevel, cfg.InstanceID)
		},
		observability.NewMetrics,
		func(cfg config.Config, log *slog.Logger) *faults.Injector { return faults.New(cfg.FaultInjection, log) },
	),
)

var PostgresModule = fx.Module("postgres",
	fx.Provide(
		newPool,
		postgres.NewUnitOfWork,
		func(u *postgres.UnitOfWork) app.UnitOfWork { return u },
		postgres.NewOutboxStore,
	),
)

func newPool(lc fx.Lifecycle, cfg config.Config, log *slog.Logger) (*pgxpool.Pool, error) {
	pool, err := postgres.NewPool(context.Background(), postgres.Config{
		URL: cfg.DatabaseURL, MaxConns: cfg.DatabaseMaxConns,
		StatementTimeout: cfg.DatabaseStatementTimeout, LockTimeout: cfg.DatabaseLockTimeout,
	})
	if err != nil {
		return nil, err
	}
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error { return postgres.PingWithRetry(ctx, pool, log) },
		OnStop: func(context.Context) error {
			pool.Close()
			log.Info("postgres pool closed")
			return nil
		},
	})
	return pool, nil
}

// Queues holds the queue URLs, resolved when the SQS module starts.
type Queues struct {
	WagerURL  string
	DLQURL    string
	EventsURL string
}

var SQSModule = fx.Module("sqs",
	fx.Provide(newSQSClient, newQueues),
)

func newSQSClient(cfg config.Config) (*sqs.Client, error) {
	return sqsx.NewClient(context.Background(), sqsx.Config{
		Region: cfg.AWSRegion, Endpoint: cfg.SQSEndpoint, AccessKeyID: cfg.AWSAccessKeyID, SecretAccessKey: cfg.AWSSecretAccessKey,
	})
}

func newQueues(lc fx.Lifecycle, client *sqs.Client, cfg config.Config, log *slog.Logger) *Queues {
	q := &Queues{}
	lc.Append(fx.Hook{OnStart: func(ctx context.Context) error {
		resolve := func(name string) (string, error) {
			backoff := 250 * time.Millisecond
			for {
				url, err := sqsx.QueueURL(ctx, client, name)
				if err == nil {
					return url, nil
				}
				log.Warn("queue not ready", slog.String("queue", name), slog.Any("error", err))
				select {
				case <-ctx.Done():
					return "", errors.Join(err, ctx.Err())
				case <-time.After(backoff):
				}
				if backoff < 2*time.Second {
					backoff *= 2
				}
			}
		}
		var err error
		if q.WagerURL, err = resolve(cfg.WagerQueue); err != nil {
			return err
		}
		if q.DLQURL, err = resolve(cfg.WagerDLQ); err != nil {
			return err
		}
		q.EventsURL, err = resolve(cfg.EventsQueue)
		return err
	}})
	return q
}

var AuthModule = fx.Module("auth",
	fx.Provide(func(lc fx.Lifecycle, cfg config.Config) *auth.Verifier {
		lc.Append(fx.Hook{OnStart: func(ctx context.Context) error { return auth.WaitForJWKS(ctx, cfg.OIDCJWKSURL) }})
		return auth.NewVerifier(auth.Config{IssuerURL: cfg.OIDCIssuerURL, JWKSURL: cfg.OIDCJWKSURL, Audience: cfg.OIDCAudience})
	}),
)

var AppModule = fx.Module("app",
	fx.Provide(
		func() app.Clock { return app.SystemClock{} },
		func() app.IDGenerator { return app.UUIDv7{} },
		func(m *observability.Metrics) app.Metrics { return m },
		func(cfg config.Config) app.WageringConfig {
			return app.WageringConfig{ConflictRetries: cfg.ConflictRetries, Schedule: app.ExponentialSchedule{
				Base: cfg.PendingBackoffBase, Max: cfg.PendingBackoffMax, MaxAttempts: cfg.PendingMaxAttempts, TTL: cfg.PendingTTL,
			}}
		},
		app.NewWageringService,
		app.NewWalletService,
	),
)

var WorkersModule = fx.Module("workers",
	fx.Provide(
		func(client *sqs.Client, q *Queues, svc *app.WageringService, cfg config.Config, log *slog.Logger, m *observability.Metrics, f *faults.Injector) *worker.Consumer {
			return worker.NewConsumer(client, svc, worker.ConsumerConfig{
				Name: cfg.ConsumerName, Workers: cfg.ConsumerWorkers, WaitTime: cfg.ConsumerWaitTime,
				VisibilityTimeout: cfg.ConsumerVisibilityTimeout, ProcessingTimeout: cfg.ConsumerProcessingTimeout,
				MaxReceiveCount: cfg.ConsumerMaxReceiveCount, BackoffBase: cfg.ConsumerBackoffBase, BackoffMax: cfg.ConsumerBackoffMax,
			}, log, m, f)
		},
		func(store *postgres.OutboxStore, client *sqs.Client, q *Queues, cfg config.Config, log *slog.Logger, m *observability.Metrics, f *faults.Injector) *worker.OutboxPublisher {
			return worker.NewOutboxPublisher(store, &lazyPublisher{client: client, queues: q}, worker.OutboxConfig{
				Owner: cfg.InstanceID + "/" + uuid.NewString()[:8], Workers: cfg.OutboxWorkers, PollInterval: cfg.OutboxPollInterval, BatchSize: cfg.OutboxBatchSize,
				Lease: cfg.OutboxLease, BackoffBase: cfg.OutboxBackoffBase, BackoffMax: cfg.OutboxBackoffMax,
			}, log, m, f)
		},
		func(svc *app.WageringService, cfg config.Config, log *slog.Logger) *worker.PendingWorker {
			return worker.NewPendingWorker(svc, worker.PendingConfig{PollInterval: cfg.PendingPollInterval, BatchSize: cfg.PendingBatchSize}, log)
		},
	),
	fx.Invoke(registerWorkers),
)

// lazyPublisher reads the events queue URL resolved at start.
type lazyPublisher struct {
	client *sqs.Client
	queues *Queues
}

func (p *lazyPublisher) PublishBatch(ctx context.Context, entries []sqsx.BatchEntry) (map[string]error, error) {
	return sqsx.NewPublisher(p.client, p.queues.EventsURL).PublishBatch(ctx, entries)
}

type workerParams struct {
	fx.In
	Lifecycle fx.Lifecycle
	Config    config.Config
	Queues    *Queues
	Consumer  *worker.Consumer
	Outbox    *worker.OutboxPublisher
	Pending   *worker.PendingWorker
}

func registerWorkers(p workerParams) {
	if p.Config.OutboxEnabled {
		p.Lifecycle.Append(fx.StartStopHook(p.Outbox.Start, p.Outbox.Stop))
	}
	if p.Config.PendingEnabled {
		p.Lifecycle.Append(fx.StartStopHook(p.Pending.Start, p.Pending.Stop))
	}
	if p.Config.ConsumerEnabled {
		p.Lifecycle.Append(fx.Hook{
			OnStart: func(context.Context) error {
				p.Consumer.SetQueues(p.Queues.WagerURL, p.Queues.DLQURL)
				p.Consumer.Start()
				return nil
			},
			OnStop: p.Consumer.Stop,
		})
	}
}

var HTTPModule = fx.Module("http",
	fx.Provide(
		func(pool *pgxpool.Pool, client *sqs.Client, q *Queues) []httpapi.ReadinessCheck {
			return []httpapi.ReadinessCheck{
				{Name: "postgres", Check: func(ctx context.Context) error { return pool.Ping(ctx) }},
				{Name: "sqs", Check: func(ctx context.Context) error { return sqsx.Ping(ctx, client, q.WagerURL) }},
			}
		},
		func(v *auth.Verifier) httpapi.TokenVerifier { return v },
		httpapi.NewAPI,
	),
	fx.Invoke(registerHTTPServer),
)

func registerHTTPServer(lc fx.Lifecycle, cfg config.Config, api *httpapi.API, log *slog.Logger) {
	srv := &http.Server{
		Addr: cfg.HTTPAddr, Handler: api.Handler(),
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second,
	}
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			ln, err := net.Listen("tcp", cfg.HTTPAddr)
			if err != nil {
				return err
			}
			log.Info("http server listening", slog.String("addr", ln.Addr().String()))
			go func() {
				if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
					log.Error("http server failed", slog.Any("error", err))
				}
			}()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			// Readiness fails first so load balancers stop routing, then
			// Shutdown stops accepting and waits for in-flight requests.
			api.SetDraining()
			err := srv.Shutdown(ctx)
			log.Info("http server stopped", slog.Any("error", err))
			return err
		},
	})
}
