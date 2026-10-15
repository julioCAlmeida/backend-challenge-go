// Command loadtest drives the HTTP API of one or more running instances and
// reports throughput, latency percentiles, errors, concurrency conflicts and
// outbox lag. It finishes by reconciling every wallet it used.
//
//	go run ./cmd/loadtest -targets http://localhost:8081,http://localhost:8082,http://localhost:8083
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

type wallet struct{ id, player string }

type op struct {
	key  string
	body []byte
}

func main() {
	targets := flag.String("targets", "http://localhost:8081,http://localhost:8082,http://localhost:8083", "comma-separated instance URLs")
	keycloak := flag.String("keycloak", "http://localhost:8180", "Keycloak base URL")
	wallets := flag.Int("wallets", 50, "number of wallets")
	ops := flag.Int("ops", 5000, "number of operations")
	concurrency := flag.Int("concurrency", 32, "concurrent clients")
	duplicates := flag.Float64("duplicates", 0.1, "fraction of requests that resend a previous operation")
	flag.Parse()

	bases := strings.Split(*targets, ",")
	client := &http.Client{Timeout: 30 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: *concurrency}}
	operator := token(*keycloak, "wallet-service")
	provider := token(*keycloak, "provider-a")

	ws := make([]wallet, *wallets)
	for i := range ws {
		player := uuid.NewString()
		body, _ := json.Marshal(map[string]any{"playerId": player, "initialBalance": map[string]string{"amount": "1000.00", "currency": "BRL"}})
		status, resp := do(client, "POST", bases[i%len(bases)]+"/wallets", operator, body, nil)
		if status != 201 {
			fail("open wallet: %d %s", status, resp)
		}
		ws[i] = wallet{id: resp["id"].(string), player: player}
	}
	conflictsBefore := sumMetric(client, bases, "concurrency_conflicts_total")

	var (
		latencies = make([]time.Duration, *ops)
		statuses  sync.Map
		next      atomic.Int64
		sent      []op
		sentMu    sync.Mutex
		maxLag    atomic.Int64
		done      = make(chan struct{})
	)
	go func() { // sample the outbox lag during the run
		t := time.NewTicker(500 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				if lag := int64(maxMetric(client, bases, "outbox_lag_seconds") * 1000); lag > maxLag.Load() {
					maxLag.Store(lag)
				}
			}
		}
	}()

	start := time.Now()
	var wg sync.WaitGroup
	for c := 0; c < *concurrency; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(next.Add(1)) - 1
				if i >= *ops {
					return
				}
				var o op
				sentMu.Lock()
				if len(sent) > 0 && rand.Float64() < *duplicates {
					o = sent[rand.IntN(len(sent))]
				} else {
					o = newOp(ws[rand.IntN(len(ws))])
					sent = append(sent, o)
				}
				sentMu.Unlock()
				t0 := time.Now()
				status, _ := do(client, "POST", bases[i%len(bases)]+"/wagering/transactions", provider, o.body, map[string]string{"Idempotency-Key": o.key})
				latencies[i] = time.Since(t0)
				v, _ := statuses.LoadOrStore(status, new(atomic.Int64))
				v.(*atomic.Int64).Add(1)
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	drainStart := time.Now()
	for maxMetric(client, bases, "outbox_pending_events") > 0 && time.Since(drainStart) < 2*time.Minute {
		time.Sleep(200 * time.Millisecond)
	}
	drain := time.Since(drainStart)
	close(done)

	inconsistent := 0
	for i, w := range ws {
		_, rec := do(client, "POST", bases[i%len(bases)]+"/wallets/"+w.id+"/reconciliation", operator, nil, nil)
		if rec["consistent"] != true {
			inconsistent++
		}
	}

	slices.Sort(latencies)
	pct := func(p float64) time.Duration { return latencies[int(float64(len(latencies)-1)*p)] }
	fmt.Printf("instances:            %d (%s)\n", len(bases), *targets)
	fmt.Printf("wallets / ops / conc: %d / %d / %d (duplicates %.0f%%)\n", *wallets, *ops, *concurrency, *duplicates*100)
	fmt.Printf("duration:             %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("throughput:           %.1f ops/s\n", float64(*ops)/elapsed.Seconds())
	fmt.Printf("latency p50/p95/p99:  %s / %s / %s (max %s)\n", pct(.50).Round(time.Microsecond*100),
		pct(.95).Round(time.Microsecond*100), pct(.99).Round(time.Microsecond*100), latencies[len(latencies)-1].Round(time.Millisecond))
	fmt.Print("http statuses:        ")
	statuses.Range(func(k, v any) bool { fmt.Printf("%v=%d ", k, v.(*atomic.Int64).Load()); return true })
	fmt.Println()
	fmt.Printf("concurrency conflicts: %.0f (retried internally)\n", sumMetric(client, bases, "concurrency_conflicts_total")-conflictsBefore)
	fmt.Printf("outbox lag:           max %dms during the run; drained %s after the last request\n", maxLag.Load(), drain.Round(time.Millisecond))
	fmt.Printf("reconciliation:       %d/%d wallets consistent\n", len(ws)-inconsistent, len(ws))
	if inconsistent > 0 {
		os.Exit(1)
	}
}

var kinds = []struct {
	kind, amount string
	weight       float64
}{{"BET", "1.00", .6}, {"WIN", "1.50", .3}, {"LOSS", "0.00", .1}}

func newOp(w wallet) op {
	r, kind, amount := rand.Float64(), "BET", "1.00"
	for _, k := range kinds {
		if r < k.weight {
			kind, amount = k.kind, k.amount
			break
		}
		r -= k.weight
	}
	ext := "load-" + uuid.NewString()
	body, _ := json.Marshal(map[string]any{
		"providerId": "provider-a", "externalTransactionId": ext, "playerId": w.player, "walletId": w.id,
		"roundId": "round-" + ext[5:13], "gameId": "load-game", "kind": kind,
		"money": map[string]string{"amount": amount, "currency": "BRL"},
	})
	return op{key: "provider-a:" + ext, body: body}
}

func do(c *http.Client, method, u, tok string, body []byte, headers map[string]string) (int, map[string]any) {
	req, _ := http.NewRequest(method, u, bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := c.Do(req)
	if err != nil {
		return 0, nil
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func token(keycloak, client string) string {
	resp, err := http.PostForm(keycloak+"/realms/wagering/protocol/openid-connect/token", url.Values{
		"grant_type": {"client_credentials"}, "client_id": {client}, "client_secret": {client + "-secret"},
	})
	if err != nil {
		fail("token: %v", err)
	}
	defer resp.Body.Close()
	var body struct {
		AccessToken string `json:"access_token"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if body.AccessToken == "" {
		fail("no token for %s (status %d)", client, resp.StatusCode)
	}
	return body.AccessToken
}

// metric reads an unlabeled gauge/counter (or the sum of its series) from /metrics.
func metric(c *http.Client, base, name string) float64 {
	resp, err := c.Get(base + "/metrics")
	if err != nil {
		return 0
	}
	defer resp.Body.Close()
	var total float64
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, name+" ") || strings.HasPrefix(line, name+"{") {
			if v, err := strconv.ParseFloat(line[strings.LastIndex(line, " ")+1:], 64); err == nil {
				total += v
			}
		}
	}
	return total
}

func sumMetric(c *http.Client, bases []string, name string) float64 {
	var s float64
	for _, b := range bases {
		s += metric(c, b, name)
	}
	return s
}

func maxMetric(c *http.Client, bases []string, name string) float64 {
	var m float64
	for _, b := range bases {
		m = max(m, metric(c, b, name))
	}
	return m
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "loadtest: "+format+"\n", args...)
	os.Exit(1)
}
