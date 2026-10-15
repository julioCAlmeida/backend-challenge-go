//go:build integration

package integration

import (
	"context"
	"net/http"
	"runtime"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"
)

// TestFxLifecycleStartsAndReleasesResources starts the full composition, then
// stops it and checks that the HTTP listener, the workers and the pool are gone.
func TestFxLifecycleStartsAndReleasesResources(t *testing.T) {
	db, q := newDB(t), newQueues(t, 3, 6*time.Second)
	baseline := runtime.NumGoroutine()
	var pool *pgxpool.Pool
	inst := startInProcess(t, appEnv(t, db, q, nil), fx.Populate(&pool))
	w := openWallet(t, inst.BaseURL, "10.00")
	if r := postOp(t, inst.BaseURL, "provider-a", op(w, "provider-a", "BET", "fx-bet", "1.00", "")); r.Status != 200 {
		t.Fatalf("%d %s", r.Status, r.Raw)
	}
	if runtime.NumGoroutine() <= baseline {
		t.Fatal("expected worker goroutines while running")
	}
	base := inst.BaseURL
	if err := inst.Stop(); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if _, err := http.Get(base + "/health/live"); err == nil {
		t.Fatal("HTTP server still accepting connections")
	}
	if _, err := pool.Acquire(context.Background()); err == nil {
		t.Fatal("pool still open after stop")
	}
	http.DefaultClient.CloseIdleConnections()
	httpClient.CloseIdleConnections()
	eventually(t, 10*time.Second, "worker goroutines finished", func() bool {
		return runtime.NumGoroutine() <= baseline+5
	})
}

// TestGracefulShutdownFinishesInFlightMessage: SIGTERM-equivalent stop while a
// message is being processed (blocked on a wallet lock). The stop waits for the
// processing, the result is committed and the message is deleted.
func TestGracefulShutdownFinishesInFlightMessage(t *testing.T) {
	db, q := newDB(t), newQueues(t, 3, 10*time.Second)
	inst := startInProcess(t, appEnv(t, db, q, map[string]string{"SQS_VISIBILITY_TIMEOUT": "10s", "SQS_PROCESSING_TIMEOUT": "8s"}))
	w := openWallet(t, inst.BaseURL, "100.00")

	ctx := context.Background()
	tx, err := db.Owner.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `SELECT 1 FROM wallets WHERE id = $1 FOR UPDATE`, w.ID); err != nil {
		t.Fatal(err)
	}
	sendMessage(t, q.WagerURL, "msg-shutdown", op(w, "provider-a", "BET", "shutdown-bet", "10.00", ""))
	eventually(t, 15*time.Second, "consumer blocked on the wallet lock", func() bool {
		return db.scalar(t, `SELECT COUNT(*) FROM pg_stat_activity WHERE datname = $1 AND wait_event_type = 'Lock'`, db.Name) > 0
	})

	stopped := make(chan error, 1)
	go func() { stopped <- inst.Stop() }()
	time.Sleep(1500 * time.Millisecond)
	select {
	case err := <-stopped:
		t.Fatalf("stop returned before the in-flight message finished: %v", err)
	default:
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-stopped:
		if err != nil {
			t.Fatalf("stop: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("stop did not finish")
	}
	if n := db.scalar(t, `SELECT COUNT(*) FROM wager_transactions WHERE external_transaction_id = 'shutdown-bet' AND status = 'PROCESSED'`); n != 1 {
		t.Fatalf("in-flight message not committed: %d", n)
	}
	visible, inFlight := approximateMessages(t, q.WagerURL)
	if visible+inFlight != 0 {
		t.Fatalf("message not deleted: visible %d in flight %d", visible, inFlight)
	}
}
