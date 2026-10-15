//go:build integration

package integration

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
)

func discardLogger() *slog.Logger { return slog.New(slog.NewJSONHandler(io.Discard, nil)) }

// cluster is a set of instances sharing one database and one set of queues.
type cluster struct {
	db    *testDB
	q     *testQueues
	bases []string
}

func (c *cluster) base(i int) string { return c.bases[i%len(c.bases)] }

func inProcessCluster(t *testing.T) *cluster {
	db, q := newDB(t), newQueues(t, 3, 6*time.Second)
	inst := startInProcess(t, appEnv(t, db, q, nil))
	return &cluster{db: db, q: q, bases: []string{inst.BaseURL}}
}

// processCluster starts n independent OS processes.
func processCluster(t *testing.T, n int) *cluster {
	db, q := newDB(t), newQueues(t, 3, 6*time.Second)
	c := &cluster{db: db, q: q}
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			inst := startProcess(t, appEnv(t, db, q, nil))
			mu.Lock()
			c.bases = append(c.bases, inst.BaseURL)
			mu.Unlock()
		}()
	}
	wg.Wait()
	if len(c.bases) != n {
		t.Fatalf("started %d of %d instances", len(c.bases), n)
	}
	return c
}

func TestConcurrencyInProcess(t *testing.T) {
	runConcurrencyScenarios(t, inProcessCluster(t))
}

func TestConcurrencyThreeIndependentProcesses(t *testing.T) {
	c := processCluster(t, 3)
	runConcurrencyScenarios(t, c)
	t.Run("http and sqs deliver the same operation concurrently", func(t *testing.T) { scenarioHTTPAndSQS(t, c) })
}

func runConcurrencyScenarios(t *testing.T, c *cluster) {
	t.Run("same bet 50 times in parallel debits once", func(t *testing.T) { scenarioSameBet50(t, c) })
	t.Run("two bets of 80.00 on 100.00", func(t *testing.T) { scenarioTwoBets80(t, c) })
	t.Run("independent wallets progress in parallel", func(t *testing.T) { scenarioIndependentWallets(t, c) })
	c.db.assertConsistent(t)
}

func scenarioSameBet50(t *testing.T, c *cluster) {
	w := openWallet(t, c.base(0), "1000.00")
	d := op(w, "provider-a", "BET", "dup-"+uuid.NewString(), "25.00", "")
	tok := token(t, "provider-a")
	var (
		wg             sync.WaitGroup
		mu             sync.Mutex
		ids            = map[string]int{}
		fresh, replays atomic.Int32
		start          = make(chan struct{})
	)
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			r, err := tryPostOp(c.base(i), tok, d)
			if err != nil || r.Status != 200 {
				t.Errorf("request %d: %v %d %s", i, err, r.Status, r.Raw)
				return
			}
			if r.Body["balance"].(map[string]any)["amount"] != "975.00" {
				t.Errorf("request %d balance %v", i, r.Body["balance"])
			}
			if r.Body["idempotentReplay"] == true {
				replays.Add(1)
			} else {
				fresh.Add(1)
			}
			mu.Lock()
			ids[r.Body["transactionId"].(string)]++
			mu.Unlock()
		}(i)
	}
	close(start)
	wg.Wait()
	if len(ids) != 1 || fresh.Load() != 1 || replays.Load() != 49 {
		t.Fatalf("ids %v fresh %d replays %d", ids, fresh.Load(), replays.Load())
	}
	if n := c.db.scalar(t, `SELECT COUNT(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'`, w.ID); n != 1 {
		t.Fatalf("debits %d", n)
	}
	if got := balanceOf(t, c.base(1), w.ID); got != "975.00" {
		t.Fatalf("balance %s", got)
	}
}

func scenarioTwoBets80(t *testing.T, c *cluster) {
	const rounds = 10
	var wg sync.WaitGroup
	for r := 0; r < rounds; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			w := openWallet(t, c.base(r), "100.00")
			bets := []wagerData{
				op(w, "provider-a", "BET", fmt.Sprintf("race-%d-a-%s", r, w.ID), "80.00", ""),
				op(w, "provider-a", "BET", fmt.Sprintf("race-%d-b-%s", r, w.ID), "80.00", ""),
			}
			results := make([]response, 2)
			var inner sync.WaitGroup
			start := make(chan struct{})
			for i := range bets {
				inner.Add(1)
				go func(i int) {
					defer inner.Done()
					<-start
					results[i] = postOp(t, c.base(r+i), "provider-a", bets[i])
				}(i)
			}
			close(start)
			inner.Wait()
			assertOneProcessedOneRejected(t, results)
			// Resending both never changes the outcome.
			for i := range bets {
				again := postOp(t, c.base(r+i+1), "provider-a", bets[i])
				if again.Status != results[i].Status || again.Body["transactionId"] != results[i].Body["transactionId"] || again.Body["idempotentReplay"] != true {
					t.Errorf("resend %d: %d %s (original %d)", i, again.Status, again.Raw, results[i].Status)
				}
			}
			if got := balanceOf(t, c.base(r), w.ID); got != "20.00" {
				t.Errorf("balance %s", got)
			}
			if n := c.db.scalar(t, `SELECT COUNT(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'`, w.ID); n != 1 {
				t.Errorf("debits %d", n)
			}
		}(r)
	}
	wg.Wait()
}

func assertOneProcessedOneRejected(t *testing.T, rs []response) {
	t.Helper()
	processed, rejected := 0, 0
	for _, r := range rs {
		switch {
		case r.Status == 200 && r.Body["status"] == "PROCESSED":
			processed++
			if r.Body["balance"].(map[string]any)["amount"] != "20.00" {
				t.Errorf("processed balance %v", r.Body["balance"])
			}
		case r.Status == 422 && r.Body["status"] == "REJECTED" && r.Body["failureCode"] == "INSUFFICIENT_FUNDS":
			rejected++
		default:
			t.Errorf("unexpected response %d %s", r.Status, r.Raw)
		}
	}
	if processed != 1 || rejected != 1 {
		t.Errorf("processed %d rejected %d", processed, rejected)
	}
}

// scenarioIndependentWallets proves there is no global lock: while a wallet is
// locked by a long transaction, operations on other wallets complete.
func scenarioIndependentWallets(t *testing.T, c *cluster) {
	ctx := context.Background()
	blocked := openWallet(t, c.base(0), "100.00")
	tx, err := c.db.Owner.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `SELECT 1 FROM wallets WHERE id = $1 FOR UPDATE`, blocked.ID); err != nil {
		t.Fatal(err)
	}
	blockedDone := make(chan response, 1)
	go func() {
		blockedDone <- postOp(t, c.base(0), "provider-a", op(blocked, "provider-a", "BET", "blocked-"+uuid.NewString(), "10.00", ""))
	}()

	// 20 other wallets x 5 bets in parallel, while the first wallet is locked.
	const wallets, bets = 20, 5
	start := time.Now()
	var wg sync.WaitGroup
	infos := make([]walletInfo, wallets)
	for i := range infos {
		infos[i] = openWallet(t, c.base(i), "100.00")
	}
	for i := range infos {
		for j := 0; j < bets; j++ {
			wg.Add(1)
			go func(i, j int) {
				defer wg.Done()
				r := postOp(t, c.base(i+j), "provider-a", op(infos[i], "provider-a", "BET", fmt.Sprintf("par-%d-%d-%s", i, j, infos[i].ID), "1.00", ""))
				if r.Status != 200 {
					t.Errorf("wallet %d bet %d: %d %s", i, j, r.Status, r.Raw)
				}
			}(i, j)
		}
	}
	wg.Wait()
	elapsed := time.Since(start)
	select {
	case r := <-blockedDone:
		t.Fatalf("operation on the locked wallet finished while locked: %d %s", r.Status, r.Raw)
	default:
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if r := <-blockedDone; r.Status != 200 {
		t.Fatalf("locked wallet after release: %d %s", r.Status, r.Raw)
	}
	for i := range infos {
		if got := balanceOf(t, c.base(i), infos[i].ID); got != "95.00" {
			t.Errorf("wallet %d balance %s", i, got)
		}
	}
	t.Logf("%d operations on %d wallets completed in %s while another wallet was locked", wallets*bets, wallets, elapsed)
}

// scenarioHTTPAndSQS sends the same operation through both entry points at the
// same time, several times: exactly one debit, one transaction.
func scenarioHTTPAndSQS(t *testing.T, c *cluster) {
	w := openWallet(t, c.base(0), "500.00")
	const ops = 10
	var wg sync.WaitGroup
	for i := 0; i < ops; i++ {
		d := op(w, "provider-a", "BET", fmt.Sprintf("cross-%d-%s", i, w.ID), "10.00", "")
		wg.Add(3)
		go func() { defer wg.Done(); sendMessage(t, c.q.WagerURL, "msg-"+d.ExternalTransactionID, d) }()
		go func(i int) {
			defer wg.Done()
			if r := postOp(t, c.base(i), "provider-a", d); r.Status != 200 {
				t.Errorf("http: %d %s", r.Status, r.Raw)
			}
		}(i)
		// The same message delivered twice (at-least-once publisher).
		go func() { defer wg.Done(); sendMessage(t, c.q.WagerURL, "msg-"+d.ExternalTransactionID, d) }()
	}
	wg.Wait()
	eventually(t, 30*time.Second, "all messages consumed", func() bool {
		return c.db.scalar(t, `SELECT COUNT(*) FROM inbox_messages WHERE message_id LIKE 'msg-cross-%' AND completed_at IS NOT NULL`) == ops
	})
	eventually(t, 15*time.Second, "queue drained", func() bool {
		v, f := approximateMessages(t, c.q.WagerURL)
		return v == 0 && f == 0
	})
	if n := c.db.scalar(t, `SELECT COUNT(*) FROM wager_transactions WHERE wallet_id = $1 AND kind = 'BET'`, w.ID); n != ops {
		t.Fatalf("transactions %d", n)
	}
	if n := c.db.scalar(t, `SELECT COUNT(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'`, w.ID); n != ops {
		t.Fatalf("debits %d", n)
	}
	if got := balanceOf(t, c.base(0), w.ID); got != "400.00" {
		t.Fatalf("balance %s", got)
	}
}
