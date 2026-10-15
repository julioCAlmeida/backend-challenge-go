//go:build integration

package integration

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/google/uuid"

	"github.com/julioCAlmeida/backend-challenge-go/internal/faults"
)

func metricValue(t *testing.T, base, name string) string {
	t.Helper()
	resp, err := http.Get(base + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.HasPrefix(line, name) {
			return strings.TrimSpace(line[strings.LastIndex(line, " "):])
		}
	}
	return ""
}

// Scenario 5: the consumer dies after the commit and before DeleteMessage. The
// message is redelivered to another instance, which answers it from the inbox.
func TestConsumerCrashAfterCommitBeforeDelete(t *testing.T) {
	db, q := newDB(t), newQueues(t, 5, 3*time.Second)
	crashing := startProcess(t, appEnv(t, db, q, map[string]string{
		"FAULT_INJECTION": faults.ConsumerAfterCommit, "SQS_VISIBILITY_TIMEOUT": "3s", "SQS_PROCESSING_TIMEOUT": "2s",
	}))
	w := openWallet(t, crashing.BaseURL, "100.00")
	d := op(w, "provider-a", "BET", "crash-"+uuid.NewString(), "30.00", "")
	sendMessage(t, q.WagerURL, "msg-crash-1", d)

	if code := crashing.WaitExit(30 * time.Second); code != 137 {
		t.Fatalf("exit code %d, want 137 (fault injection)", code)
	}
	// Committed but not deleted.
	if n := db.scalar(t, `SELECT COUNT(*) FROM inbox_messages WHERE message_id = 'msg-crash-1' AND completed_at IS NOT NULL`); n != 1 {
		t.Fatalf("inbox rows %d", n)
	}
	if n := db.scalar(t, `SELECT COUNT(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'`, w.ID); n != 1 {
		t.Fatalf("debits before redelivery %d", n)
	}
	visible, inFlight := approximateMessages(t, q.WagerURL)
	if visible+inFlight != 1 {
		t.Fatalf("message must still be in the queue: visible %d in flight %d", visible, inFlight)
	}

	healthy := startProcess(t, appEnv(t, db, q, nil))
	eventually(t, 30*time.Second, "redelivered message deleted", func() bool {
		v, f := approximateMessages(t, q.WagerURL)
		return v == 0 && f == 0
	})
	if got := metricValue(t, healthy.BaseURL, `inbox_duplicate_messages_total{consumer="wager-transactions-consumer"}`); got != "1" {
		t.Fatalf("duplicate deliveries observed by the inbox: %q", got)
	}
	if n := db.scalar(t, `SELECT COUNT(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'`, w.ID); n != 1 {
		t.Fatalf("debits after redelivery %d", n)
	}
	if got := balanceOf(t, healthy.BaseURL, w.ID); got != "70.00" {
		t.Fatalf("balance %s", got)
	}
	db.assertConsistent(t)
}

// Scenario 6: two publishers compete for the same outbox; a publisher dies
// after publishing and before confirming, another one after claiming. Every
// event ends up published, republications keep the eventId (and are absorbed
// by the FIFO deduplication).
func TestOutboxCompetingPublishersAndRecovery(t *testing.T) {
	db, q := newDB(t), newQueues(t, 5, 6*time.Second)
	writer := startProcess(t, appEnv(t, db, q, map[string]string{"OUTBOX_ENABLED": "false", "SQS_CONSUMER_ENABLED": "false"}))
	for i := 0; i < 20; i++ {
		w := openWallet(t, writer.BaseURL, "50.00")
		postOp(t, writer.BaseURL, "provider-a", op(w, "provider-a", "BET", "ob-"+w.ID, "5.00", ""))
	}
	total := db.scalar(t, `SELECT COUNT(*) FROM outbox_events`)
	if total != 80 { // per wallet: opening (Processed + BalanceChanged) and bet (Processed + BalanceChanged)
		t.Fatalf("outbox events %d, want 80", total)
	}

	// A publisher that crashes right after the first SQS send.
	crashAfterPublish := launchProcess(t, appEnv(t, db, q, map[string]string{
		"FAULT_INJECTION": faults.OutboxAfterPublish, "SQS_CONSUMER_ENABLED": "false", "OUTBOX_BATCH_SIZE": "5"}))
	if code := crashAfterPublish.WaitExit(30 * time.Second); code != 137 {
		t.Fatalf("exit code %d", code)
	}
	if n := db.scalar(t, `SELECT COUNT(*) FROM outbox_events WHERE published_at IS NOT NULL`); n != 0 {
		t.Fatalf("published before confirmation: %d", n)
	}
	// A publisher that crashes after leasing a batch.
	crashAfterClaim := launchProcess(t, appEnv(t, db, q, map[string]string{
		"FAULT_INJECTION": faults.OutboxAfterClaim, "SQS_CONSUMER_ENABLED": "false", "OUTBOX_BATCH_SIZE": "5"}))
	if code := crashAfterClaim.WaitExit(30 * time.Second); code != 137 {
		t.Fatalf("exit code %d", code)
	}
	if n := db.scalar(t, `SELECT COUNT(*) FROM outbox_events WHERE locked_until > now()`); n == 0 {
		t.Fatal("expected abandoned leases")
	}

	// Two healthy publishers compete for the remaining work, including the
	// abandoned leases once they expire.
	p1 := startProcess(t, appEnv(t, db, q, map[string]string{"SQS_CONSUMER_ENABLED": "false", "OUTBOX_BATCH_SIZE": "3"}))
	startProcess(t, appEnv(t, db, q, map[string]string{"SQS_CONSUMER_ENABLED": "false", "OUTBOX_BATCH_SIZE": "3"}))
	eventually(t, 40*time.Second, "all events published", func() bool {
		return db.scalar(t, `SELECT COUNT(*) FROM outbox_events WHERE published_at IS NULL`) == 0
	})
	if n := db.scalar(t, `SELECT COUNT(*) FROM outbox_events WHERE attempts > 1`); n == 0 {
		t.Fatal("expected republished events (attempts > 1)")
	}

	msgs := drain(t, q.EventsURL, 5*time.Second)
	seen := map[string]int{}
	for _, m := range msgs {
		id := aws.ToString(m.MessageAttributes["eventId"].StringValue)
		if !strings.Contains(aws.ToString(m.Body), `"eventId":"`+id+`"`) {
			t.Fatalf("eventId attribute %s does not match the payload", id)
		}
		seen[id]++
	}
	var missing int
	rows, _ := db.Owner.Query(context.Background(), `SELECT event_id::text FROM outbox_events`)
	for rows.Next() {
		var id string
		_ = rows.Scan(&id)
		if seen[id] == 0 {
			missing++
		}
	}
	rows.Close()
	if missing != 0 {
		t.Fatalf("%d events never reached the queue", missing)
	}
	for id, n := range seen {
		if n > 1 {
			t.Errorf("event %s delivered %d times despite FIFO deduplication", id, n)
		}
	}
	t.Logf("published %d events; %d were republished after crashes", len(seen),
		db.scalar(t, `SELECT COUNT(*) FROM outbox_events WHERE attempts > 1`))
	if lag := metricValue(t, p1.BaseURL, "outbox_pending_events"); lag != "0" {
		t.Logf("outbox_pending_events = %s", lag)
	}
}

// Scenario 7: REFUND/ROLLBACK delivered before their reference.
func TestReversalBeforeReference(t *testing.T) {
	db, q := newDB(t), newQueues(t, 3, 6*time.Second)
	inst := startInProcess(t, appEnv(t, db, q, map[string]string{"PENDING_MAX_ATTEMPTS": "4", "PENDING_BACKOFF_BASE": "100ms", "PENDING_BACKOFF_MAX": "200ms"}))
	w := openWallet(t, inst.BaseURL, "100.00")

	t.Run("refund via SQS resolves when the bet arrives over HTTP", func(t *testing.T) {
		refund := op(w, "provider-a", "REFUND", "refund-1", "40.00", "bet-1")
		sendMessage(t, q.WagerURL, "msg-refund-1", refund)
		path := "/providers/provider-a/wagering/transactions/refund-1"
		eventually(t, 15*time.Second, "refund pending", func() bool {
			r := call(t, inst.BaseURL, "GET", path, token(t, "provider-a"), nil, nil)
			return r.Status == 200 && r.Body["status"] == "PENDING_REFERENCE"
		})
		if r := postOp(t, inst.BaseURL, "provider-a", op(w, "provider-a", "BET", "bet-1", "40.00", "")); r.Status != 200 {
			t.Fatalf("bet: %d %s", r.Status, r.Raw)
		}
		eventually(t, 15*time.Second, "refund processed", func() bool {
			r := call(t, inst.BaseURL, "GET", path, token(t, "provider-a"), nil, nil)
			return r.Body["status"] == "PROCESSED"
		})
		if got := balanceOf(t, inst.BaseURL, w.ID); got != "100.00" {
			t.Fatalf("balance %s", got)
		}
		if n := db.scalar(t, `SELECT COUNT(*) FROM outbox_events WHERE event_type = 'WagerTransactionPendingReference'`); n != 1 {
			t.Fatalf("pending events %d", n)
		}
	})

	t.Run("http answers 202 while pending and the reversal expires", func(t *testing.T) {
		rb := op(w, "provider-a", "ROLLBACK", "rollback-x", "10.00", "never-arrives")
		r := postOp(t, inst.BaseURL, "provider-a", rb)
		if r.Status != 202 || r.Body["status"] != "PENDING_REFERENCE" {
			t.Fatalf("pending: %d %s", r.Status, r.Raw)
		}
		eventually(t, 20*time.Second, "rollback expired", func() bool {
			r := postOp(t, inst.BaseURL, "provider-a", rb) // replay shows the persisted state
			return r.Status == 422 && r.Body["failureCode"] == "REFERENCE_NOT_FOUND" && r.Body["idempotentReplay"] == true
		})
		if n := db.scalar(t, `SELECT COUNT(*) FROM outbox_events WHERE event_type = 'WagerTransactionRejected'
			AND payload::jsonb -> 'data' ->> 'externalTransactionId' = 'rollback-x'`); n != 1 {
			t.Fatalf("rejected events %d", n)
		}
	})

	t.Run("second reversal of the same bet is rejected", func(t *testing.T) {
		r := postOp(t, inst.BaseURL, "provider-a", op(w, "provider-a", "ROLLBACK", "rollback-2", "40.00", "bet-1"))
		if r.Status != 422 || r.Body["failureCode"] != "REFERENCE_ALREADY_REVERSED" {
			t.Fatalf("%d %s", r.Status, r.Raw)
		}
	})
	db.assertConsistent(t)
}

// Scenario 8: abrupt restart. Idempotency, pending work and balances survive;
// a committed PENDING row (as left by an async accept) is resumed by another
// instance; a crash after commit and before the HTTP response is answered by
// a replay on another instance.
func TestRestartPreservesIdempotencyAndPendingWork(t *testing.T) {
	db, q := newDB(t), newQueues(t, 3, 6*time.Second)
	first := startProcess(t, appEnv(t, db, q, nil))
	w := openWallet(t, first.BaseURL, "200.00")
	bet := op(w, "provider-a", "BET", "restart-bet", "50.00", "")
	original := postOp(t, first.BaseURL, "provider-a", bet)
	if original.Status != 200 {
		t.Fatalf("bet %d %s", original.Status, original.Raw)
	}
	pending := postOp(t, first.BaseURL, "provider-a", op(w, "provider-a", "REFUND", "restart-refund", "20.00", "restart-late-bet"))
	if pending.Status != 202 {
		t.Fatalf("refund %d %s", pending.Status, pending.Raw)
	}
	first.Kill()

	// A PENDING row committed by a process that died before executing it.
	orphan := uuid.New()
	_, err := db.Owner.Exec(context.Background(), `INSERT INTO wager_transactions (id, origin, kind, status, provider_id,
		external_transaction_id, idempotency_key, payload_hash, wallet_id, player_id, round_id, game_id, amount_minor, currency,
		created_at, updated_at) VALUES ($1, 'EXTERNAL', 'WIN', 'PENDING', 'provider-a', 'orphan-win', 'provider-a:orphan-win',
		repeat('0', 64), $2, $3, 'round-1', 'fortune-chimp', 1000, 'BRL', now(), now())`, orphan, w.ID, w.PlayerID)
	if err != nil {
		t.Fatal(err)
	}

	crashing := startProcess(t, appEnv(t, db, q, map[string]string{"FAULT_INJECTION": faults.HTTPAfterCommit, "PENDING_ENABLED": "false"}))
	second := startProcess(t, appEnv(t, db, q, nil))

	replay := postOp(t, second.BaseURL, "provider-a", bet)
	if replay.Body["transactionId"] != original.Body["transactionId"] || replay.Body["idempotentReplay"] != true ||
		replay.Body["balance"].(map[string]any)["amount"] != "150.00" {
		t.Fatalf("replay after restart: %s", replay.Raw)
	}
	eventually(t, 15*time.Second, "orphan PENDING resumed", func() bool {
		r := call(t, second.BaseURL, "GET", "/wagering/transactions/"+orphan.String(), token(t, "provider-a"), nil, nil)
		return r.Body["status"] == "PROCESSED"
	})

	// Crash after commit, before the response.
	win := op(w, "provider-a", "WIN", "restart-win", "5.00", "")
	if _, err := tryPostOp(crashing.BaseURL, token(t, "provider-a"), win); err == nil {
		t.Fatal("expected the connection to drop")
	}
	crashing.WaitExit(10 * time.Second)
	retry := postOp(t, second.BaseURL, "provider-a", win)
	if retry.Status != 200 || retry.Body["idempotentReplay"] != true {
		t.Fatalf("retry after crash: %d %s", retry.Status, retry.Raw)
	}

	// The late reference arrives: the pending refund is resolved.
	postOp(t, second.BaseURL, "provider-a", op(w, "provider-a", "BET", "restart-late-bet", "20.00", ""))
	eventually(t, 15*time.Second, "pending refund resolved after restart", func() bool {
		r := call(t, second.BaseURL, "GET", "/providers/provider-a/wagering/transactions/restart-refund", token(t, "provider-a"), nil, nil)
		return r.Body["status"] == "PROCESSED"
	})
	// 200 - 50 + 10 (orphan) + 5 - 20 + 20 = 165
	if got := balanceOf(t, second.BaseURL, w.ID); got != "165.00" {
		t.Fatalf("balance %s", got)
	}
	rec := call(t, second.BaseURL, "POST", "/wallets/"+w.ID+"/reconciliation", token(t, "wallet-service"), nil, nil)
	if rec.Body["consistent"] != true || rec.Body["checkedEntries"].(float64) != 6 {
		t.Fatalf("reconciliation %s", rec.Raw)
	}
	db.assertConsistent(t)
}

// Temporary unavailability of PostgreSQL and SQS.
func TestDependencyOutages(t *testing.T) {
	db, q := newDB(t), newQueues(t, 3, 6*time.Second)
	pgProxy := newProxy(t, hostOf(db.AppURL))
	sqsProxy := newProxy(t, hostOf(sqsEndpoint))
	env := appEnv(t, db, q, map[string]string{
		"DATABASE_URL": viaProxy(db.AppURL, pgProxy.Addr()), "SQS_ENDPOINT": "http://" + sqsProxy.Addr(),
		"SQS_CONSUMER_ENABLED": "false", "DATABASE_STATEMENT_TIMEOUT": "3s",
	})
	inst := startInProcess(t, env)
	w := openWallet(t, inst.BaseURL, "100.00")

	t.Run("postgres outage answers 503 and recovers", func(t *testing.T) {
		pgProxy.Cut()
		r := postOp(t, inst.BaseURL, "provider-a", op(w, "provider-a", "BET", "outage-bet", "10.00", ""))
		if r.Status != 503 || r.Body["error"].(map[string]any)["code"] != "TEMPORARILY_UNAVAILABLE" {
			t.Fatalf("during outage: %d %s", r.Status, r.Raw)
		}
		if rr := call(t, inst.BaseURL, "GET", "/health/ready", "", nil, nil); rr.Status != 503 {
			t.Fatalf("readiness during outage: %d", rr.Status)
		}
		pgProxy.Restore()
		eventually(t, 15*time.Second, "request succeeds after recovery", func() bool {
			r := postOp(t, inst.BaseURL, "provider-a", op(w, "provider-a", "BET", "outage-bet", "10.00", ""))
			return r.Status == 200
		})
		if got := balanceOf(t, inst.BaseURL, w.ID); got != "90.00" {
			t.Fatalf("balance %s", got)
		}
	})

	t.Run("sqs outage keeps events in the outbox until it recovers", func(t *testing.T) {
		eventually(t, 15*time.Second, "outbox drained", func() bool {
			return db.scalar(t, `SELECT COUNT(*) FROM outbox_events WHERE published_at IS NULL`) == 0
		})
		sqsProxy.Cut()
		postOp(t, inst.BaseURL, "provider-a", op(w, "provider-a", "WIN", "outage-win", "1.00", ""))
		eventually(t, 15*time.Second, "publication retried", func() bool {
			return db.scalar(t, `SELECT COUNT(*) FROM outbox_events WHERE published_at IS NULL AND attempts >= 2 AND last_error IS NOT NULL`) > 0
		})
		sqsProxy.Restore()
		eventually(t, 20*time.Second, "events published after recovery", func() bool {
			return db.scalar(t, `SELECT COUNT(*) FROM outbox_events WHERE published_at IS NULL`) == 0
		})
	})
}

// Invalid messages, payload conflicts and exhausted transient retries reach the DLQ.
func TestDeadLetterQueue(t *testing.T) {
	db, q := newDB(t), newQueues(t, 2, 3*time.Second)
	pgProxy := newProxy(t, hostOf(db.AppURL))
	inst := startInProcess(t, appEnv(t, db, q, map[string]string{
		"DATABASE_URL": viaProxy(db.AppURL, pgProxy.Addr()), "SQS_VISIBILITY_TIMEOUT": "3s", "SQS_PROCESSING_TIMEOUT": "2s",
		"SQS_BACKOFF_BASE": "1s", "SQS_BACKOFF_MAX": "1s", "SQS_MAX_RECEIVE_COUNT": "2", "DATABASE_STATEMENT_TIMEOUT": "1s",
	}))
	w := openWallet(t, inst.BaseURL, "100.00")

	sendRaw(t, q.WagerURL, "g1", uuid.NewString(), `{"messageId":"broken"`)
	sendRaw(t, q.WagerURL, "g1", uuid.NewString(), `{"messageId":"m-opening","type":"WagerTransactionRequested","data":{"providerId":"provider-a","externalTransactionId":"x","idempotencyKey":"k","playerId":"`+w.PlayerID+`","walletId":"`+w.ID+`","roundId":"r","gameId":"g","kind":"OPENING","money":{"amount":"1.00","currency":"BRL"}}}`)
	unknown := op(walletInfo{ID: uuid.NewString(), PlayerID: w.PlayerID}, "provider-a", "BET", "unknown-wallet", "1.00", "")
	sendMessage(t, q.WagerURL, "m-unknown-wallet", unknown)
	good := op(w, "provider-a", "BET", "dlq-good", "1.00", "")
	sendMessage(t, q.WagerURL, "m-reused", good)
	eventually(t, 15*time.Second, "first message processed", func() bool {
		return db.scalar(t, `SELECT COUNT(*) FROM inbox_messages WHERE message_id = 'm-reused'`) == 1
	})
	conflicting := good
	conflicting.Money = map[string]any{"amount": "2.00", "currency": "BRL"}
	sendMessage(t, q.WagerURL, "m-reused", conflicting) // same messageId, different content

	dlq := drainN(t, q.DLQURL, 4, 20*time.Second)
	reasons := map[string]string{}
	for _, m := range dlq {
		reasons[aws.ToString(m.Body)[:min(40, len(aws.ToString(m.Body)))]] = aws.ToString(m.MessageAttributes["failureCode"].StringValue)
	}
	if len(dlq) != 4 {
		t.Fatalf("dlq messages %d: %v", len(dlq), reasons)
	}
	if got := balanceOf(t, inst.BaseURL, w.ID); got != "99.00" {
		t.Fatalf("balance %s", got)
	}

	// Transient failures: retried with backoff, then moved by the redrive policy.
	pgProxy.Cut()
	sendMessage(t, q.WagerURL, "m-transient", op(w, "provider-a", "BET", "dlq-transient", "1.00", ""))
	redriven := drainN(t, q.DLQURL, 1, 30*time.Second)
	pgProxy.Restore()
	if len(redriven) != 1 || !strings.Contains(aws.ToString(redriven[0].Body), "m-transient") {
		t.Fatalf("redriven %d", len(redriven))
	}
	if _, ok := redriven[0].MessageAttributes["failureReason"]; ok {
		t.Fatal("transient message must arrive through the redrive policy, not explicitly")
	}
	retries := metricValue(t, inst.BaseURL, `retries_total{component="sqs-consumer"}`)
	if retries == "" || retries == "0" {
		t.Fatalf("retries metric %q", retries)
	}
	if n := metricValue(t, inst.BaseURL, `sqs_dlq_messages_total{reason="invalid-message"}`); n != "2" {
		t.Fatalf("invalid-message DLQ metric %q", n)
	}
	t.Logf("dead-lettered: %v; redriven after %s retries", reasons, retries)
}
