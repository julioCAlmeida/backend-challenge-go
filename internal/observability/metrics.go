package observability

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/julioCAlmeida/backend-challenge-go/internal/app"
)

// Metrics holds every Prometheus collector of the service. Each instance owns
// its registry, so several Fx apps can run in one test process.
type Metrics struct {
	Registry *prometheus.Registry

	transactions          *prometheus.CounterVec
	replays               *prometheus.CounterVec
	idempotencyConflicts  *prometheus.CounterVec
	duplicateMessages     *prometheus.CounterVec
	concurrencyConflicts  *prometheus.CounterVec
	retries               *prometheus.CounterVec
	processingDuration    *prometheus.HistogramVec
	reconciliations       *prometheus.CounterVec
	reconciliationDiverge prometheus.Counter
	pendingExpired        prometheus.Counter

	DLQ                  *prometheus.CounterVec
	SQSMessages          *prometheus.CounterVec
	OutboxPublished      prometheus.Counter
	OutboxPublishFailed  prometheus.Counter
	OutboxLagSeconds     prometheus.Gauge
	OutboxPending        prometheus.Gauge
	OutboxPublishLatency prometheus.Histogram
	HTTPRequests         *prometheus.CounterVec
	HTTPDuration         *prometheus.HistogramVec
	AuthFailures         *prometheus.CounterVec
}

var _ app.Metrics = (*Metrics)(nil)

func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	f := func(c prometheus.Collector) { reg.MustRegister(c) }
	m := &Metrics{Registry: reg}
	buckets := []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10}

	m.transactions = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "wager_transactions_total", Help: "Wager transactions by final status."}, []string{"channel", "kind", "status"})
	m.replays = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "wager_idempotent_replays_total", Help: "Requests answered from the persisted result."}, []string{"channel"})
	m.idempotencyConflicts = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "wager_idempotency_conflicts_total", Help: "Idempotency key or external id reused with different content."}, []string{"channel"})
	m.duplicateMessages = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "inbox_duplicate_messages_total", Help: "Messages already completed in the inbox."}, []string{"consumer"})
	m.concurrencyConflicts = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "concurrency_conflicts_total", Help: "Deadlocks, serialization failures and lost version checks."}, []string{"operation"})
	m.retries = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "retries_total", Help: "Retries by component."}, []string{"component"})
	m.processingDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "wager_processing_duration_seconds", Help: "Latency of wager processing.", Buckets: buckets}, []string{"channel"})
	m.reconciliations = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "reconciliations_total", Help: "Reconciliations by result."}, []string{"consistent"})
	m.reconciliationDiverge = prometheus.NewCounter(prometheus.CounterOpts{Name: "reconciliation_divergences_total", Help: "Wallets whose stored balance differs from the ledger."})
	m.pendingExpired = prometheus.NewCounter(prometheus.CounterOpts{Name: "pending_reference_expired_total", Help: "Pending references rejected after exhausting attempts."})
	m.DLQ = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "sqs_dlq_messages_total", Help: "Messages sent to the DLQ by the consumer."}, []string{"reason"})
	m.SQSMessages = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "sqs_messages_total", Help: "Consumed messages by outcome."}, []string{"outcome"})
	m.OutboxPublished = prometheus.NewCounter(prometheus.CounterOpts{Name: "outbox_published_total", Help: "Published outbox events."})
	m.OutboxPublishFailed = prometheus.NewCounter(prometheus.CounterOpts{Name: "outbox_publish_failures_total", Help: "Failed outbox publications."})
	m.OutboxLagSeconds = prometheus.NewGauge(prometheus.GaugeOpts{Name: "outbox_lag_seconds", Help: "Age of the oldest unpublished outbox event."})
	m.OutboxPending = prometheus.NewGauge(prometheus.GaugeOpts{Name: "outbox_pending_events", Help: "Unpublished outbox events."})
	m.OutboxPublishLatency = prometheus.NewHistogram(prometheus.HistogramOpts{Name: "outbox_publish_delay_seconds", Help: "Delay between event occurrence and publication.", Buckets: buckets})
	m.HTTPRequests = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "http_requests_total", Help: "HTTP requests."}, []string{"route", "status"})
	m.HTTPDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "http_request_duration_seconds", Help: "HTTP latency.", Buckets: buckets}, []string{"route"})
	m.AuthFailures = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "auth_failures_total", Help: "Rejected authentications/authorizations."}, []string{"reason"})

	for _, c := range []prometheus.Collector{m.transactions, m.replays, m.idempotencyConflicts, m.duplicateMessages,
		m.concurrencyConflicts, m.retries, m.processingDuration, m.reconciliations, m.reconciliationDiverge,
		m.pendingExpired, m.DLQ, m.SQSMessages, m.OutboxPublished, m.OutboxPublishFailed, m.OutboxLagSeconds,
		m.OutboxPending, m.OutboxPublishLatency, m.HTTPRequests, m.HTTPDuration, m.AuthFailures} {
		f(c)
	}
	return m
}

// Handler exposes the registry in the Prometheus text format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})
}

func (m *Metrics) TransactionCompleted(channel, kind, status string) {
	m.transactions.WithLabelValues(channel, kind, status).Inc()
}
func (m *Metrics) IdempotentReplay(channel string) { m.replays.WithLabelValues(channel).Inc() }
func (m *Metrics) IdempotencyConflict(channel string) {
	m.idempotencyConflicts.WithLabelValues(channel).Inc()
}
func (m *Metrics) DuplicateMessage(consumer string) {
	m.duplicateMessages.WithLabelValues(consumer).Inc()
}
func (m *Metrics) ConcurrencyConflict(op string) { m.concurrencyConflicts.WithLabelValues(op).Inc() }
func (m *Metrics) Retry(component string)        { m.retries.WithLabelValues(component).Inc() }
func (m *Metrics) ProcessingDuration(channel string, d time.Duration) {
	m.processingDuration.WithLabelValues(channel).Observe(d.Seconds())
}
func (m *Metrics) ReconciliationChecked(consistent bool) {
	if consistent {
		m.reconciliations.WithLabelValues("true").Inc()
		return
	}
	m.reconciliations.WithLabelValues("false").Inc()
	m.reconciliationDiverge.Inc()
}
func (m *Metrics) PendingReferenceExpired() { m.pendingExpired.Inc() }
