//go:build integration

package integration

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"
)

func TestReconciliationAndLedgerPagination(t *testing.T) {
	db, q := newDB(t), newQueues(t, 3, 6*time.Second)
	inst := startInProcess(t, appEnv(t, db, q, nil))
	op := token(t, "wallet-service")
	w := openWallet(t, inst.BaseURL, "1000.00")
	for i := 0; i < 7; i++ {
		postOp(t, inst.BaseURL, "provider-a", opFor(w, "BET", fmt.Sprintf("page-%d", i), "1.00"))
	}
	// LOSS does not create a ledger entry nor change the version.
	if r := postOp(t, inst.BaseURL, "provider-a", opFor(w, "LOSS", "page-loss", "0.00")); r.Status != 200 {
		t.Fatalf("loss %d %s", r.Status, r.Raw)
	}

	// Cursor pagination: 8 entries (opening + 7 bets) in pages of 3.
	var seen []float64
	cursor := ""
	for page := 0; page < 5; page++ {
		path := "/wallets/" + w.ID + "/ledger?limit=3"
		if cursor != "" {
			path += "&cursor=" + url.QueryEscape(cursor)
		}
		r := call(t, inst.BaseURL, "GET", path, op, nil, nil)
		if r.Status != 200 {
			t.Fatalf("ledger %d %s", r.Status, r.Raw)
		}
		for _, e := range r.Body["entries"].([]any) {
			seen = append(seen, e.(map[string]any)["walletVersion"].(float64))
		}
		next, _ := r.Body["nextCursor"].(string)
		if next == "" {
			break
		}
		cursor = next
	}
	if len(seen) != 8 {
		t.Fatalf("entries %v", seen)
	}
	for i, v := range seen {
		if v != float64(i+1) {
			t.Fatalf("unstable order: %v", seen)
		}
	}
	if r := call(t, inst.BaseURL, "GET", "/wallets/"+w.ID+"/ledger?cursor=garbage", op, nil, nil); r.Status != 400 {
		t.Fatalf("bad cursor %d", r.Status)
	}

	rec := call(t, inst.BaseURL, "POST", "/wallets/"+w.ID+"/reconciliation", op, nil, nil)
	if rec.Body["consistent"] != true || rec.Body["checkedEntries"].(float64) != 8 ||
		rec.Body["storedBalance"].(map[string]any)["amount"] != "993.00" {
		t.Fatalf("reconciliation %s", rec.Raw)
	}

	// Corrupt the balance behind the application's back (protections disabled
	// by the schema owner) and check that the divergence is reported without
	// being "fixed".
	ctx := context.Background()
	for _, sql := range []string{
		`ALTER TABLE wallets DISABLE TRIGGER USER`,
		`UPDATE wallets SET balance_minor = balance_minor + 500 WHERE id = '` + w.ID + `'`,
		`ALTER TABLE wallets ENABLE TRIGGER USER`,
	} {
		if _, err := db.Owner.Exec(ctx, sql); err != nil {
			t.Fatal(err)
		}
	}
	rec = call(t, inst.BaseURL, "POST", "/wallets/"+w.ID+"/reconciliation", op, nil, nil)
	if rec.Body["consistent"] != false || rec.Body["difference"].(map[string]any)["amount"] != "5.00" ||
		rec.Body["calculatedBalance"].(map[string]any)["amount"] != "993.00" {
		t.Fatalf("divergence not reported: %s", rec.Raw)
	}
	if got := metricValue(t, inst.BaseURL, "reconciliation_divergences_total"); got != "1" {
		t.Fatalf("divergence metric %q", got)
	}
	if got := balanceOf(t, inst.BaseURL, w.ID); got != "998.00" {
		t.Fatalf("reconciliation must not change the balance, got %s", got)
	}
	version := db.scalar(t, `SELECT version FROM wallets WHERE id = $1`, w.ID)
	if version != 8 {
		t.Fatalf("version %d (LOSS must not bump it)", version)
	}
}

func opFor(w walletInfo, kind, ext, amount string) wagerData {
	return op(w, "provider-a", kind, ext, amount, "")
}
