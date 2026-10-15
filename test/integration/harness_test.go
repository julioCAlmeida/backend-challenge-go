//go:build integration

// Package integration runs the service against real PostgreSQL, Keycloak and
// LocalStack (started by `docker compose up -d postgres keycloak localstack`).
// Every test gets its own database and its own FIFO queues, so tests are
// isolated from each other and from the compose application instances.
package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"

	"github.com/julioCAlmeida/backend-challenge-go/internal/bootstrap"
	"github.com/julioCAlmeida/backend-challenge-go/internal/config"
	"github.com/julioCAlmeida/backend-challenge-go/internal/infra/postgres"
	"github.com/julioCAlmeida/backend-challenge-go/internal/infra/sqsx"
)

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

var (
	pgAdminURL    = envOr("IT_PG_ADMIN_URL", "postgres://wagering_owner:wagering_owner_local@localhost:55432/wagering?sslmode=disable")
	pgAppUser     = envOr("IT_PG_APP_USER", "wallet_app")
	pgAppPassword = envOr("IT_PG_APP_PASSWORD", "wallet_app_local")
	keycloakURL   = envOr("IT_KEYCLOAK_URL", "http://localhost:8180")
	oidcIssuer    = envOr("IT_OIDC_ISSUER", "http://keycloak:8080/realms/wagering")
	sqsEndpoint   = envOr("IT_SQS_ENDPOINT", "http://localhost:4566")

	serviceBinary string
)

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "wallet-it-*")
	if err != nil {
		panic(err)
	}
	serviceBinary = filepath.Join(dir, "wallet-service")
	if runtime.GOOS == "windows" {
		serviceBinary += ".exe"
	}
	build := exec.Command("go", "build", "-o", serviceBinary, "github.com/julioCAlmeida/backend-challenge-go/cmd/wallet-service")
	build.Stdout, build.Stderr = os.Stdout, os.Stderr
	if err := build.Run(); err != nil {
		fmt.Fprintln(os.Stderr, "building service binary:", err)
		os.Exit(1)
	}
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

// ---------------------------------------------------------------------------
// PostgreSQL

type testDB struct {
	Name     string
	OwnerURL string
	AppURL   string
	Owner    *pgxpool.Pool
}

func withDatabase(raw, db, user, password string) string {
	u, err := url.Parse(raw)
	if err != nil {
		panic(err)
	}
	u.Path = "/" + db
	if user != "" {
		u.User = url.UserPassword(user, password)
	}
	return u.String()
}

// newDB creates an isolated database and applies the migrations.
func newDB(t *testing.T) *testDB {
	t.Helper()
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, pgAdminURL)
	if err != nil {
		t.Fatalf("connect admin (is `docker compose up -d postgres keycloak localstack` running?): %v", err)
	}
	defer admin.Close(ctx)
	name := "it_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:16]
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	db := &testDB{Name: name, OwnerURL: withDatabase(pgAdminURL, name, "", ""), AppURL: withDatabase(pgAdminURL, name, pgAppUser, pgAppPassword)}
	if err := postgres.MigrateUp(db.OwnerURL); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if db.Owner, err = pgxpool.New(ctx, db.OwnerURL); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		db.Owner.Close()
		c, err := pgx.Connect(context.Background(), pgAdminURL)
		if err == nil {
			_, _ = c.Exec(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
			c.Close(context.Background())
		}
	})
	return db
}

func (db *testDB) scalar(t *testing.T, sql string, args ...any) int64 {
	t.Helper()
	var v int64
	if err := db.Owner.QueryRow(context.Background(), sql, args...).Scan(&v); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return v
}

// assertConsistent checks stored balance = credits - debits for every wallet.
func (db *testDB) assertConsistent(t *testing.T) {
	t.Helper()
	n := db.scalar(t, `SELECT COUNT(*) FROM wallets w WHERE w.balance_minor <> COALESCE((
		SELECT SUM(CASE direction WHEN 'CREDIT' THEN amount_minor ELSE -amount_minor END)
		FROM wallet_ledger_entries e WHERE e.wallet_id = w.id), 0)`)
	if n != 0 {
		t.Fatalf("%d wallet(s) diverge from their ledger", n)
	}
	if neg := db.scalar(t, `SELECT COUNT(*) FROM wallets WHERE balance_minor < 0`); neg != 0 {
		t.Fatalf("%d negative balances", neg)
	}
}

// ---------------------------------------------------------------------------
// SQS

var (
	sqsOnce   sync.Once
	sqsClient *sqs.Client
)

func sqsc(t *testing.T) *sqs.Client {
	t.Helper()
	sqsOnce.Do(func() {
		c, err := sqsx.NewClient(context.Background(), sqsx.Config{Region: "us-east-1", Endpoint: sqsEndpoint, AccessKeyID: "integration-tests", SecretAccessKey: "x"})
		if err != nil {
			panic(err)
		}
		sqsClient = c
	})
	return sqsClient
}

type testQueues struct {
	Wager, DLQ, Events          string
	WagerURL, DLQURL, EventsURL string
}

func newQueues(t *testing.T, maxReceive int, visibility time.Duration) *testQueues {
	t.Helper()
	ctx := context.Background()
	p := "it-" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	q := &testQueues{Wager: p + "-wager.fifo", DLQ: p + "-wager-dlq.fifo", Events: p + "-events.fifo"}
	var err error
	if q.WagerURL, err = sqsx.Ensure(ctx, sqsc(t), sqsx.QueueSpec{Name: q.Wager, DLQName: q.DLQ, MaxReceiveCount: maxReceive, VisibilityTimeout: visibility}); err != nil {
		t.Fatalf("create queues (is LocalStack running?): %v", err)
	}
	if q.DLQURL, err = sqsx.QueueURL(ctx, sqsc(t), q.DLQ); err != nil {
		t.Fatal(err)
	}
	if q.EventsURL, err = sqsx.Ensure(ctx, sqsc(t), sqsx.QueueSpec{Name: q.Events}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for _, u := range []string{q.WagerURL, q.DLQURL, q.EventsURL} {
			_, _ = sqsc(t).DeleteQueue(context.Background(), &sqs.DeleteQueueInput{QueueUrl: aws.String(u)})
		}
	})
	return q
}

type wagerData struct {
	ProviderID                     string         `json:"providerId"`
	ExternalTransactionID          string         `json:"externalTransactionId"`
	IdempotencyKey                 string         `json:"idempotencyKey,omitempty"`
	PlayerID                       string         `json:"playerId"`
	WalletID                       string         `json:"walletId"`
	RoundID                        string         `json:"roundId"`
	GameID                         string         `json:"gameId"`
	Kind                           string         `json:"kind"`
	Money                          map[string]any `json:"money"`
	ReferenceExternalTransactionID string         `json:"referenceExternalTransactionId,omitempty"`
}

func op(w walletInfo, provider, kind, ext, amount, ref string) wagerData {
	return wagerData{ProviderID: provider, ExternalTransactionID: ext, PlayerID: w.PlayerID, WalletID: w.ID,
		RoundID: "round-1", GameID: "fortune-chimp", Kind: kind, Money: map[string]any{"amount": amount, "currency": "BRL"},
		ReferenceExternalTransactionID: ref}
}

// sendMessage publishes a WagerTransactionRequested envelope.
func sendMessage(t *testing.T, queueURL, messageID string, d wagerData) {
	t.Helper()
	d.IdempotencyKey = d.ProviderID + ":" + d.ExternalTransactionID
	body, _ := json.Marshal(map[string]any{"messageId": messageID, "type": "WagerTransactionRequested",
		"occurredAt": time.Now().UTC().Format(time.RFC3339Nano), "data": d})
	sendRaw(t, queueURL, d.WalletID, uuid.NewString(), string(body))
}

func sendRaw(t *testing.T, queueURL, group, dedup, body string) {
	t.Helper()
	_, err := sqsc(t).SendMessage(context.Background(), &sqs.SendMessageInput{
		QueueUrl: aws.String(queueURL), MessageBody: aws.String(body),
		MessageGroupId: aws.String(group), MessageDeduplicationId: aws.String(dedup),
	})
	if err != nil {
		t.Fatal(err)
	}
}

// drain receives every message currently available in a queue (deleting them).
func drain(t *testing.T, queueURL string, wait time.Duration) []types.Message {
	t.Helper()
	var out []types.Message
	deadline := time.Now().Add(wait)
	for time.Now().Before(deadline) {
		res, err := sqsc(t).ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{
			QueueUrl: aws.String(queueURL), MaxNumberOfMessages: 10, WaitTimeSeconds: 1, VisibilityTimeout: 30,
			MessageAttributeNames: []string{"All"},
		})
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range res.Messages {
			out = append(out, m)
			_, _ = sqsc(t).DeleteMessage(context.Background(), &sqs.DeleteMessageInput{QueueUrl: aws.String(queueURL), ReceiptHandle: m.ReceiptHandle})
		}
	}
	return out
}

func approximateMessages(t *testing.T, queueURL string) (visible, inFlight int) {
	t.Helper()
	out, err := sqsc(t).GetQueueAttributes(context.Background(), &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(queueURL), AttributeNames: []types.QueueAttributeName{
			types.QueueAttributeNameApproximateNumberOfMessages, types.QueueAttributeNameApproximateNumberOfMessagesNotVisible},
	})
	if err != nil {
		t.Fatal(err)
	}
	fmt.Sscan(out.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessages)], &visible)
	fmt.Sscan(out.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessagesNotVisible)], &inFlight)
	return
}

// ---------------------------------------------------------------------------
// Keycloak

var (
	tokenMu    sync.Mutex
	tokenCache = map[string]string{}
)

// token obtains an access token with the client_credentials grant.
func token(t *testing.T, clientID string) string {
	t.Helper()
	tokenMu.Lock()
	defer tokenMu.Unlock()
	if tok, ok := tokenCache[clientID]; ok && clientID != "provider-a-shortlived" {
		return tok
	}
	tok := fetchToken(t, clientID)
	tokenCache[clientID] = tok
	return tok
}

func fetchToken(t *testing.T, clientID string) string {
	t.Helper()
	form := url.Values{"grant_type": {"client_credentials"}, "client_id": {clientID}, "client_secret": {clientID + "-secret"}}
	resp, err := http.PostForm(keycloakURL+"/realms/wagering/protocol/openid-connect/token", form)
	if err != nil {
		t.Fatalf("keycloak: %v", err)
	}
	defer resp.Body.Close()
	var body struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil || body.AccessToken == "" {
		t.Fatalf("token for %s: status %d %v", clientID, resp.StatusCode, err)
	}
	return body.AccessToken
}

// ---------------------------------------------------------------------------
// Service instances

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// appEnv is the configuration of an instance bound to a test database and queues.
func appEnv(t *testing.T, db *testDB, q *testQueues, extra map[string]string) map[string]string {
	m := map[string]string{
		"INSTANCE_ID": "it-" + uuid.NewString()[:6], "HTTP_ADDR": fmt.Sprintf("127.0.0.1:%d", freePort(t)), "LOG_LEVEL": "warn",
		"DATABASE_URL": db.AppURL, "DATABASE_MAX_CONNS": "30", "DATABASE_LOCK_TIMEOUT": "5s",
		"AWS_REGION": "us-east-1", "SQS_ENDPOINT": sqsEndpoint, "AWS_ACCESS_KEY_ID": "wallet-service", "AWS_SECRET_ACCESS_KEY": "x",
		"SQS_WAGER_QUEUE": q.Wager, "SQS_WAGER_DLQ": q.DLQ, "SQS_EVENTS_QUEUE": q.Events,
		"SQS_WAIT_TIME": "1s", "SQS_VISIBILITY_TIMEOUT": "6s", "SQS_PROCESSING_TIMEOUT": "5s",
		"SQS_BACKOFF_BASE": "1s", "SQS_BACKOFF_MAX": "2s", "SQS_MAX_RECEIVE_COUNT": "3",
		"OUTBOX_POLL_INTERVAL": "100ms", "OUTBOX_LEASE": "3s", "OUTBOX_BACKOFF_BASE": "200ms", "OUTBOX_BACKOFF_MAX": "1s",
		"PENDING_POLL_INTERVAL": "100ms", "PENDING_BACKOFF_BASE": "200ms", "PENDING_BACKOFF_MAX": "500ms",
		"PENDING_MAX_ATTEMPTS": "50", "PENDING_TTL": "10m",
		"OIDC_ISSUER_URL": oidcIssuer, "OIDC_JWKS_URL": keycloakURL + "/realms/wagering/protocol/openid-connect/certs",
		"SHUTDOWN_TIMEOUT": "15s", "START_TIMEOUT": "30s",
	}
	for k, v := range extra {
		m[k] = v
	}
	return m
}

type instance struct {
	t       *testing.T
	BaseURL string
	App     *fx.App
	cmd     *exec.Cmd
	exited  chan struct{}
	exitErr error
}

func cfgFrom(t *testing.T, env map[string]string) config.Config {
	t.Helper()
	cfg, err := config.LoadFrom(func(k string) string { return env[k] })
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

// startInProcess runs the Fx application inside the test process.
func startInProcess(t *testing.T, env map[string]string, opts ...fx.Option) *instance {
	t.Helper()
	cfg := cfgFrom(t, env)
	app := bootstrap.New(cfg, opts...)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := app.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	inst := &instance{t: t, BaseURL: "http://" + cfg.HTTPAddr, App: app}
	t.Cleanup(func() { inst.Stop() })
	return inst
}

// Stop runs the graceful shutdown of an in-process instance.
func (i *instance) Stop() error {
	if i.App == nil {
		return nil
	}
	app := i.App
	i.App = nil
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	return app.Stop(ctx)
}

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

// startProcess runs the service binary as an independent OS process (own
// memory, own connection pool). It waits for readiness unless the process
// exits first.
func startProcess(t *testing.T, env map[string]string) *instance {
	t.Helper()
	inst := launchProcess(t, env)
	deadline := time.Now().Add(40 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-inst.exited:
			t.Fatalf("process exited during startup: %v", inst.exitErr)
		default:
		}
		resp, err := http.Get(inst.BaseURL + "/health/ready")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				return inst
			}
		}
		time.Sleep(150 * time.Millisecond)
	}
	t.Fatal("process did not become ready")
	return nil
}

// launchProcess starts the process without waiting for readiness (used for
// processes expected to crash at a fault-injection point).
func launchProcess(t *testing.T, env map[string]string) *instance {
	t.Helper()
	cmd := exec.Command(serviceBinary)
	cmd.Env = os.Environ()
	for k, v := range env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	logs := &syncBuffer{}
	cmd.Stdout, cmd.Stderr = logs, logs
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	inst := &instance{t: t, BaseURL: "http://" + env["HTTP_ADDR"], cmd: cmd, exited: make(chan struct{})}
	go func() { inst.exitErr = cmd.Wait(); close(inst.exited) }()
	t.Cleanup(func() {
		inst.Kill()
		if t.Failed() {
			logs.mu.Lock()
			out := logs.b.String()
			logs.mu.Unlock()
			if len(out) > 4000 {
				out = out[len(out)-4000:]
			}
			t.Logf("process %s logs (tail):\n%s", env["INSTANCE_ID"], out)
		}
	})
	return inst
}

// Kill terminates the process abruptly (SIGKILL / TerminateProcess).
func (i *instance) Kill() {
	if i.cmd == nil {
		return
	}
	select {
	case <-i.exited:
		return
	default:
	}
	_ = i.cmd.Process.Kill()
	<-i.exited
}

// WaitExit waits for a process to exit by itself (fault injection) and returns its code.
func (i *instance) WaitExit(timeout time.Duration) int {
	i.t.Helper()
	select {
	case <-i.exited:
		return i.cmd.ProcessState.ExitCode()
	case <-time.After(timeout):
		i.t.Fatal("process did not exit")
		return -1
	}
}

// ---------------------------------------------------------------------------
// HTTP helpers

var httpClient = &http.Client{Timeout: 30 * time.Second}

type response struct {
	Status int
	Body   map[string]any
	Raw    string
}

func call(t *testing.T, base, method, path, tok string, body any, headers map[string]string) response {
	t.Helper()
	r, err := tryCall(base, method, path, tok, body, headers)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	return r
}

func tryCall(base, method, path, tok string, body any, headers map[string]string) (response, error) {
	var rd io.Reader
	if body != nil {
		switch b := body.(type) {
		case string:
			rd = strings.NewReader(b)
		default:
			raw, _ := json.Marshal(b)
			rd = bytes.NewReader(raw)
		}
	}
	req, _ := http.NewRequest(method, base+path, rd)
	if tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return response{}, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	out := response{Status: resp.StatusCode, Raw: string(raw)}
	_ = json.Unmarshal(raw, &out.Body)
	return out, nil
}

type walletInfo struct {
	ID       string
	PlayerID string
}

func openWallet(t *testing.T, base, balance string) walletInfo {
	t.Helper()
	player := uuid.NewString()
	r := call(t, base, "POST", "/wallets", token(t, "wallet-service"),
		map[string]any{"playerId": player, "initialBalance": map[string]any{"amount": balance, "currency": "BRL"}}, nil)
	if r.Status != 201 {
		t.Fatalf("open wallet: %d %s", r.Status, r.Raw)
	}
	return walletInfo{ID: r.Body["id"].(string), PlayerID: player}
}

func postOp(t *testing.T, base, clientID string, d wagerData) response {
	t.Helper()
	r, err := tryPostOp(base, token(t, clientID), d)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func tryPostOp(base, tok string, d wagerData) (response, error) {
	key := d.ProviderID + ":" + d.ExternalTransactionID
	d.IdempotencyKey = ""
	return tryCall(base, "POST", "/wagering/transactions", tok, d, map[string]string{"Idempotency-Key": key})
}

func balanceOf(t *testing.T, base, walletID string) string {
	t.Helper()
	r := call(t, base, "GET", "/wallets/"+walletID, token(t, "wallet-service"), nil, nil)
	if r.Status != 200 {
		t.Fatalf("get wallet: %d %s", r.Status, r.Raw)
	}
	return r.Body["balance"].(map[string]any)["amount"].(string)
}

func eventually(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", what)
}

// drainN receives (and deletes) messages until n arrived or the timeout expires.
func drainN(t *testing.T, queueURL string, n int, timeout time.Duration) []types.Message {
	t.Helper()
	var out []types.Message
	deadline := time.Now().Add(timeout)
	for len(out) < n && time.Now().Before(deadline) {
		out = append(out, drain(t, queueURL, time.Second)...)
	}
	return out
}
