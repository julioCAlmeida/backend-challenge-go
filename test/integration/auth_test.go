//go:build integration

package integration

import (
	"encoding/base64"
	"strings"
	"testing"
	"time"
)

func TestAuthentication(t *testing.T) {
	db, q := newDB(t), newQueues(t, 3, 6*time.Second)
	inst := startInProcess(t, appEnv(t, db, q, nil))
	w := openWallet(t, inst.BaseURL, "100.00")
	d := op(w, "provider-a", "BET", "auth-bet", "10.00", "")
	headers := map[string]string{"Idempotency-Key": "provider-a:auth-bet"}

	valid := token(t, "provider-a")
	parts := strings.Split(valid, ".")
	payload, _ := base64.RawURLEncoding.DecodeString(parts[1])
	escalated := strings.Replace(string(payload), `"provider_id":"provider-a"`, `"provider_id":"provider-b"`, 1)
	tampered := parts[0] + "." + base64.RawURLEncoding.EncodeToString([]byte(escalated)) + "." + parts[2]
	noneAlg := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none","typ":"JWT"}`)) + "." + parts[1] + "."

	short := fetchToken(t, "provider-a-shortlived")
	cases := []struct {
		name, token, code string
		wait              time.Duration
	}{
		{"missing token", "", "UNAUTHENTICATED", 0},
		{"garbage token", "not-a-jwt", "INVALID_TOKEN", 0},
		{"tampered payload", tampered, "INVALID_TOKEN", 0},
		{"alg none", noneAlg, "INVALID_TOKEN", 0},
		{"wrong audience", token(t, "other-audience-client"), "INVALID_TOKEN", 0},
		{"expired token", short, "TOKEN_EXPIRED", 7 * time.Second},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if c.wait > 0 {
				// The short-lived token is accepted until it expires.
				if r := call(t, inst.BaseURL, "GET", "/providers/provider-a/wagering/transactions/none", c.token, nil, nil); r.Status != 404 {
					t.Fatalf("fresh short-lived token: %d %s", r.Status, r.Raw)
				}
				time.Sleep(c.wait)
			}
			r := call(t, inst.BaseURL, "POST", "/wagering/transactions", c.token, d, headers)
			if r.Status != 401 || r.Body["error"].(map[string]any)["code"] != c.code {
				t.Fatalf("got %d %s", r.Status, r.Raw)
			}
		})
	}
	// No financial effect from any rejected call.
	if n := db.scalar(t, `SELECT COUNT(*) FROM wager_transactions WHERE origin = 'EXTERNAL'`); n != 0 {
		t.Fatalf("transactions created by unauthenticated calls: %d", n)
	}
	if got := balanceOf(t, inst.BaseURL, w.ID); got != "100.00" {
		t.Fatalf("balance %s", got)
	}
}

func TestAuthorizationAndProviderIsolation(t *testing.T) {
	db, q := newDB(t), newQueues(t, 3, 6*time.Second)
	inst := startInProcess(t, appEnv(t, db, q, nil))
	base := inst.BaseURL
	w := openWallet(t, base, "100.00")

	a := postOp(t, base, "provider-a", op(w, "provider-a", "BET", "shared-ext", "10.00", ""))
	if a.Status != 200 {
		t.Fatalf("provider-a bet: %d %s", a.Status, a.Raw)
	}
	aID := a.Body["transactionId"].(string)
	ledgerBefore := db.scalar(t, `SELECT COUNT(*) FROM wallet_ledger_entries`)

	t.Run("wallet operations are internal only", func(t *testing.T) {
		for _, c := range []struct{ method, path string }{
			{"POST", "/wallets"}, {"GET", "/wallets/" + w.ID}, {"GET", "/wallets/" + w.ID + "/ledger"},
			{"POST", "/wallets/" + w.ID + "/reconciliation"},
		} {
			for _, client := range []string{"provider-a", "no-role-client"} {
				body := any(nil)
				if c.method == "POST" && c.path == "/wallets" {
					body = map[string]any{"playerId": "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1", "initialBalance": map[string]any{"amount": "1.00", "currency": "BRL"}}
				}
				if r := call(t, base, c.method, c.path, token(t, client), body, nil); r.Status != 403 {
					t.Errorf("%s %s as %s: %d", c.method, c.path, client, r.Status)
				}
			}
		}
	})

	t.Run("the internal service cannot submit provider operations", func(t *testing.T) {
		r := postOp(t, base, "wallet-service", op(w, "provider-a", "WIN", "op-by-internal", "1000.00", ""))
		if r.Status != 403 {
			t.Fatalf("%d %s", r.Status, r.Raw)
		}
	})

	t.Run("a provider cannot act as another provider", func(t *testing.T) {
		r := postOp(t, base, "provider-b", op(w, "provider-a", "WIN", "impersonation", "1000.00", ""))
		if r.Status != 403 || r.Body["error"].(map[string]any)["code"] != "PROVIDER_MISMATCH" {
			t.Fatalf("%d %s", r.Status, r.Raw)
		}
		// Replaying provider-a's operation with provider-b's credentials does not
		// reveal provider-a's result either.
		r = postOp(t, base, "provider-b", op(w, "provider-a", "BET", "shared-ext", "10.00", ""))
		if r.Status != 403 || strings.Contains(r.Raw, aID) {
			t.Fatalf("replay as provider-b: %d %s", r.Status, r.Raw)
		}
	})

	t.Run("reads are scoped to the authenticated provider", func(t *testing.T) {
		if r := call(t, base, "GET", "/wagering/transactions/"+aID, token(t, "provider-b"), nil, nil); r.Status != 404 {
			t.Fatalf("provider-b by id: %d %s", r.Status, r.Raw)
		}
		if r := call(t, base, "GET", "/providers/provider-a/wagering/transactions/shared-ext", token(t, "provider-b"), nil, nil); r.Status != 403 {
			t.Fatalf("provider-b on provider-a path: %d", r.Status)
		}
		if r := call(t, base, "GET", "/providers/provider-b/wagering/transactions/shared-ext", token(t, "provider-b"), nil, nil); r.Status != 404 {
			t.Fatalf("provider-b own path: %d", r.Status)
		}
		if r := call(t, base, "GET", "/wagering/transactions/"+aID, token(t, "provider-a"), nil, nil); r.Status != 200 {
			t.Fatalf("owner read: %d", r.Status)
		}
		if r := call(t, base, "GET", "/wagering/transactions/"+aID, token(t, "wallet-service"), nil, nil); r.Status != 200 {
			t.Fatalf("internal read: %d", r.Status)
		}
		if r := call(t, base, "GET", "/wagering/transactions/"+aID, token(t, "no-role-client"), nil, nil); r.Status != 403 {
			t.Fatalf("no-role read: %d", r.Status)
		}
	})

	t.Run("idempotency namespaces are per provider", func(t *testing.T) {
		// provider-b uses the same external id and key under its own identity:
		// a distinct operation, not a replay of provider-a's.
		r := postOp(t, base, "provider-b", op(w, "provider-b", "BET", "shared-ext", "10.00", ""))
		if r.Status != 200 || r.Body["idempotentReplay"] != false || r.Body["transactionId"] == aID {
			t.Fatalf("%d %s", r.Status, r.Raw)
		}
	})

	if n := db.scalar(t, `SELECT COUNT(*) FROM wallet_ledger_entries`); n != ledgerBefore+1 {
		t.Fatalf("unauthorized calls changed the ledger: %d -> %d", ledgerBefore, n)
	}
	if got := balanceOf(t, base, w.ID); got != "80.00" {
		t.Fatalf("balance %s", got)
	}
}

func TestHealthEndpointsArePublic(t *testing.T) {
	db, q := newDB(t), newQueues(t, 3, 6*time.Second)
	inst := startInProcess(t, appEnv(t, db, q, nil))
	for _, p := range []string{"/health/live", "/health/ready"} {
		if r := call(t, inst.BaseURL, "GET", p, "", nil, nil); r.Status != 200 {
			t.Fatalf("%s: %d %s", p, r.Status, r.Raw)
		}
	}
}
