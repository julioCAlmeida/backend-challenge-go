// Package httpapi exposes the HTTP contract.
package httpapi

import (
	"context"
	"log/slog"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/julioCAlmeida/backend-challenge-go/internal/app"
	"github.com/julioCAlmeida/backend-challenge-go/internal/auth"
	"github.com/julioCAlmeida/backend-challenge-go/internal/contract"
	"github.com/julioCAlmeida/backend-challenge-go/internal/domain/events"
	"github.com/julioCAlmeida/backend-challenge-go/internal/domain/money"
	"github.com/julioCAlmeida/backend-challenge-go/internal/domain/wager"
	"github.com/julioCAlmeida/backend-challenge-go/internal/domain/wallet"
	"github.com/julioCAlmeida/backend-challenge-go/internal/faults"
	"github.com/julioCAlmeida/backend-challenge-go/internal/observability"
)

const maxBodyBytes = 64 << 10

// TokenVerifier validates bearer tokens.
type TokenVerifier interface {
	Verify(ctx context.Context, raw string) (auth.Principal, error)
}

// ReadinessCheck reports whether a dependency is usable.
type ReadinessCheck struct {
	Name  string
	Check func(ctx context.Context) error
}

// API holds the handlers.
type API struct {
	wallets  *app.WalletService
	wagering *app.WageringService
	verifier TokenVerifier
	log      *slog.Logger
	metrics  *observability.Metrics
	faults   *faults.Injector
	checks   []ReadinessCheck
	draining atomic.Bool
}

func NewAPI(wallets *app.WalletService, wagering *app.WageringService, verifier TokenVerifier, log *slog.Logger,
	metrics *observability.Metrics, f *faults.Injector, checks []ReadinessCheck) *API {
	return &API{wallets: wallets, wagering: wagering, verifier: verifier, log: log, metrics: metrics, faults: f, checks: checks}
}

// SetDraining makes readiness fail while the server shuts down.
func (a *API) SetDraining() { a.draining.Store(true) }

// Handler builds the router.
func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/live", a.live)
	mux.HandleFunc("GET /health/ready", a.ready)
	mux.Handle("GET /metrics", a.metrics.Handler())

	mux.HandleFunc("POST /wallets", a.authenticated(operatorOnly, a.openWallet))
	mux.HandleFunc("GET /wallets/{walletId}", a.authenticated(operatorOnly, a.getWallet))
	mux.HandleFunc("GET /wallets/{walletId}/ledger", a.authenticated(operatorOnly, a.getLedger))
	mux.HandleFunc("POST /wallets/{walletId}/reconciliation", a.authenticated(operatorOnly, a.reconcile))

	mux.HandleFunc("POST /wagering/transactions", a.authenticated(providerOnly, a.postTransaction))
	mux.HandleFunc("GET /wagering/transactions/{transactionId}", a.authenticated(providerOrOperator, a.getTransaction))
	mux.HandleFunc("GET /providers/{providerId}/wagering/transactions/{externalTransactionId}", a.authenticated(providerOrOperator, a.getProviderTransaction))

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeError(w, r, http.StatusNotFound, "ROUTE_NOT_FOUND", "route not found")
	})
	return a.observe(mux)
}

func (a *API) live(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (a *API) ready(w http.ResponseWriter, r *http.Request) {
	if a.draining.Load() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "draining"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	results := map[string]string{}
	ok := true
	for _, c := range a.checks {
		if err := c.Check(ctx); err != nil {
			ok = false
			results[c.Name] = "unavailable"
			a.log.WarnContext(ctx, "readiness check failed", slog.String("dependency", c.Name), slog.Any("error", err))
			continue
		}
		results[c.Name] = "ok"
	}
	if !ok {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "unavailable", "checks": results})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ready", "checks": results})
}

// ---- wallets (internal service only) ----

type openWalletRequest struct {
	PlayerID       string          `json:"playerId"`
	InitialBalance *contract.Money `json:"initialBalance"`
}

func (a *API) openWallet(w http.ResponseWriter, r *http.Request) {
	var body openWalletRequest
	if err := contract.DecodeStrict(http.MaxBytesReader(w, r.Body, maxBodyBytes), &body); err != nil {
		writeAppError(w, r, a.log, err)
		return
	}
	player, err := uuid.Parse(body.PlayerID)
	if err != nil || len(body.PlayerID) != 36 || player == uuid.Nil {
		writeError(w, r, http.StatusBadRequest, "INVALID_REQUEST", "playerId must be a UUID")
		return
	}
	initial, err := body.InitialBalance.ToDomain()
	if err != nil {
		writeAppError(w, r, a.log, err)
		return
	}
	snap, err := a.wallets.Open(r.Context(), app.OpenWalletCommand{
		PlayerID: player, InitialBalance: initial,
		Meta: events.Meta{CorrelationID: w.Header().Get(headerCorrelationID)},
	})
	if err != nil {
		writeAppError(w, r, a.log, err)
		return
	}
	w.Header().Set("Location", "/wallets/"+snap.ID.String())
	writeJSON(w, http.StatusCreated, contract.NewWalletResponse(snap))
}

func pathUUID(w http.ResponseWriter, r *http.Request, name string) (uuid.UUID, bool) {
	v := r.PathValue(name)
	id, err := uuid.Parse(v)
	if err != nil || len(v) != 36 {
		writeError(w, r, http.StatusBadRequest, "INVALID_REQUEST", name+" must be a UUID")
		return uuid.Nil, false
	}
	return id, true
}

func (a *API) getWallet(w http.ResponseWriter, r *http.Request) {
	id, ok := pathUUID(w, r, "walletId")
	if !ok {
		return
	}
	snap, err := a.wallets.Get(observability.WithLogAttrs(r.Context(), slog.String("walletId", id.String())), id)
	if err != nil {
		writeAppError(w, r, a.log, err)
		return
	}
	writeJSON(w, http.StatusOK, contract.NewWalletResponse(snap))
}

type ledgerEntryResponse struct {
	ID            uuid.UUID      `json:"id"`
	WalletID      uuid.UUID      `json:"walletId"`
	TransactionID uuid.UUID      `json:"transactionId"`
	Direction     string         `json:"direction"`
	Amount        contract.Money `json:"amount"`
	BalanceBefore contract.Money `json:"balanceBefore"`
	BalanceAfter  contract.Money `json:"balanceAfter"`
	WalletVersion int64          `json:"walletVersion"`
	CreatedAt     time.Time      `json:"createdAt"`
}

type ledgerResponse struct {
	WalletID   uuid.UUID             `json:"walletId"`
	Entries    []ledgerEntryResponse `json:"entries"`
	NextCursor *string               `json:"nextCursor"`
}

func (a *API) getLedger(w http.ResponseWriter, r *http.Request) {
	id, ok := pathUUID(w, r, "walletId")
	if !ok {
		return
	}
	limit := app.DefaultLedgerLimit
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > app.MaxLedgerLimit {
			writeError(w, r, http.StatusBadRequest, "INVALID_LIMIT", "limit must be an integer between 1 and 200")
			return
		}
		limit = n
	}
	page, err := a.wallets.Ledger(r.Context(), id, r.URL.Query().Get("cursor"), limit)
	if err != nil {
		writeAppError(w, r, a.log, err)
		return
	}
	resp := ledgerResponse{WalletID: id, Entries: make([]ledgerEntryResponse, 0, len(page.Entries))}
	for _, e := range page.Entries {
		resp.Entries = append(resp.Entries, toLedgerEntry(e))
	}
	if page.NextCursor != "" {
		resp.NextCursor = &page.NextCursor
	}
	writeJSON(w, http.StatusOK, resp)
}

func toLedgerEntry(e wallet.LedgerEntrySnapshot) ledgerEntryResponse {
	return ledgerEntryResponse{
		ID: e.ID, WalletID: e.WalletID, TransactionID: e.TransactionID, Direction: string(e.Direction),
		Amount: contract.FromDomain(e.Amount), BalanceBefore: contract.FromDomain(e.BalanceBefore),
		BalanceAfter: contract.FromDomain(e.BalanceAfter), WalletVersion: e.WalletVersion, CreatedAt: e.CreatedAt,
	}
}

type reconciliationResponse struct {
	WalletID          uuid.UUID      `json:"walletId"`
	StoredBalance     contract.Money `json:"storedBalance"`
	CalculatedBalance contract.Money `json:"calculatedBalance"`
	Difference        contract.Money `json:"difference"`
	Consistent        bool           `json:"consistent"`
	CheckedEntries    int64          `json:"checkedEntries"`
}

func (a *API) reconcile(w http.ResponseWriter, r *http.Request) {
	id, ok := pathUUID(w, r, "walletId")
	if !ok {
		return
	}
	res, err := a.wallets.Reconcile(observability.WithLogAttrs(r.Context(), slog.String("walletId", id.String())), id)
	if err != nil {
		writeAppError(w, r, a.log, err)
		return
	}
	writeJSON(w, http.StatusOK, reconciliationResponse{
		WalletID: res.WalletID, StoredBalance: contract.FromDomain(res.StoredBalance),
		CalculatedBalance: contract.FromDomain(res.CalculatedBalance), Difference: contract.FromDomain(res.Difference),
		Consistent: res.Consistent, CheckedEntries: res.CheckedEntries,
	})
}

// ---- wagering ----

// TransactionResult is the answer to POST /wagering/transactions.
type TransactionResult struct {
	TransactionID    uuid.UUID       `json:"transactionId"`
	Status           string          `json:"status"`
	Balance          *contract.Money `json:"balance,omitempty"`
	FailureCode      string          `json:"failureCode,omitempty"`
	IdempotentReplay bool            `json:"idempotentReplay"`
}

func (a *API) postTransaction(w http.ResponseWriter, r *http.Request) {
	p, _ := auth.FromContext(r.Context())
	key := r.Header.Get("Idempotency-Key")
	if key == "" {
		writeError(w, r, http.StatusBadRequest, "MISSING_IDEMPOTENCY_KEY", "the Idempotency-Key header is required")
		return
	}
	var body contract.WagerFields
	if err := contract.DecodeStrict(http.MaxBytesReader(w, r.Body, maxBodyBytes), &body); err != nil {
		writeAppError(w, r, a.log, err)
		return
	}
	// The provider identity comes from the token, never from the body.
	if body.ProviderID != p.ProviderID {
		a.metrics.AuthFailures.WithLabelValues("provider-mismatch").Inc()
		writeError(w, r, http.StatusForbidden, "PROVIDER_MISMATCH", "providerId does not match the authenticated provider")
		return
	}
	req, err := body.ToRequest(key)
	if err != nil {
		writeAppError(w, r, a.log, err)
		return
	}
	ctx := observability.WithLogAttrs(r.Context(), slog.String("walletId", req.WalletID.String()))
	res, err := a.wagering.Process(ctx, app.ProcessCommand{
		Request: req, Channel: app.ChannelHTTP,
		Meta: events.Meta{CorrelationID: w.Header().Get(headerCorrelationID)},
	})
	if err != nil {
		writeAppError(w, r, a.log, err)
		return
	}
	a.faults.Crash(faults.HTTPAfterCommit)
	writeJSON(w, statusFor(res.Transaction.Status), TransactionResult{
		TransactionID: res.Transaction.ID, Status: string(res.Transaction.Status),
		Balance: moneyPtr(res.Transaction.ResultBalance), FailureCode: string(res.Transaction.FailureCode),
		IdempotentReplay: res.Replay,
	})
}

// statusFor maps a persisted outcome to the HTTP status:
// PROCESSED 200, PENDING/PENDING_REFERENCE 202, REJECTED 422, FAILED 500.
func statusFor(s wager.Status) int {
	switch s {
	case wager.StatusProcessed:
		return http.StatusOK
	case wager.StatusPending, wager.StatusPendingReference:
		return http.StatusAccepted
	case wager.StatusRejected:
		return http.StatusUnprocessableEntity
	}
	return http.StatusInternalServerError
}

func moneyPtr(m *money.Money) *contract.Money {
	if m == nil {
		return nil
	}
	c := contract.FromDomain(*m)
	return &c
}

// TransactionView is the full representation of a wager transaction.
type TransactionView struct {
	TransactionID                  uuid.UUID       `json:"transactionId"`
	Origin                         string          `json:"origin"`
	Kind                           string          `json:"kind"`
	Status                         string          `json:"status"`
	ProviderID                     string          `json:"providerId,omitempty"`
	ExternalTransactionID          string          `json:"externalTransactionId,omitempty"`
	WalletID                       uuid.UUID       `json:"walletId"`
	PlayerID                       uuid.UUID       `json:"playerId"`
	RoundID                        string          `json:"roundId,omitempty"`
	GameID                         string          `json:"gameId,omitempty"`
	Money                          contract.Money  `json:"money"`
	ReferenceExternalTransactionID string          `json:"referenceExternalTransactionId,omitempty"`
	ReferenceTransactionID         *uuid.UUID      `json:"referenceTransactionId,omitempty"`
	FailureCode                    string          `json:"failureCode,omitempty"`
	Balance                        *contract.Money `json:"balance,omitempty"`
	ReferenceAttempts              int             `json:"referenceAttempts"`
	NextAttemptAt                  *time.Time      `json:"nextAttemptAt,omitempty"`
	CreatedAt                      time.Time       `json:"createdAt"`
	UpdatedAt                      time.Time       `json:"updatedAt"`
	CompletedAt                    *time.Time      `json:"completedAt,omitempty"`
}

func toView(s wager.Snapshot) TransactionView {
	return TransactionView{
		TransactionID: s.ID, Origin: string(s.Origin), Kind: string(s.Kind), Status: string(s.Status),
		ProviderID: s.ProviderID, ExternalTransactionID: s.ExternalTransactionID, WalletID: s.WalletID,
		PlayerID: s.PlayerID, RoundID: s.RoundID, GameID: s.GameID, Money: contract.FromDomain(s.Money),
		ReferenceExternalTransactionID: s.ReferenceExternalTransactionID, ReferenceTransactionID: s.ReferenceTransactionID,
		FailureCode: string(s.FailureCode), Balance: moneyPtr(s.ResultBalance), ReferenceAttempts: s.ReferenceAttempts,
		NextAttemptAt: s.NextAttemptAt, CreatedAt: s.CreatedAt, UpdatedAt: s.UpdatedAt, CompletedAt: s.CompletedAt,
	}
}

func (a *API) getTransaction(w http.ResponseWriter, r *http.Request) {
	p, _ := auth.FromContext(r.Context())
	id, ok := pathUUID(w, r, "transactionId")
	if !ok {
		return
	}
	snap, err := a.wagering.GetTransaction(r.Context(), id)
	if err != nil {
		writeAppError(w, r, a.log, err)
		return
	}
	// A provider only sees its own transactions; others look nonexistent.
	if !p.IsOperator() && snap.ProviderID != p.ProviderID {
		writeAppError(w, r, a.log, wager.ErrTransactionNotFound)
		return
	}
	writeJSON(w, http.StatusOK, toView(snap))
}

func (a *API) getProviderTransaction(w http.ResponseWriter, r *http.Request) {
	p, _ := auth.FromContext(r.Context())
	providerID := r.PathValue("providerId")
	if !p.IsOperator() && providerID != p.ProviderID {
		a.metrics.AuthFailures.WithLabelValues("provider-mismatch").Inc()
		writeError(w, r, http.StatusForbidden, "PROVIDER_MISMATCH", "providers can only read their own transactions")
		return
	}
	snap, err := a.wagering.GetByExternalID(r.Context(), providerID, r.PathValue("externalTransactionId"))
	if err != nil {
		writeAppError(w, r, a.log, err)
		return
	}
	writeJSON(w, http.StatusOK, toView(snap))
}
