package events_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/julioCAlmeida/backend-challenge-go/internal/domain/events"
	"github.com/julioCAlmeida/backend-challenge-go/internal/domain/money"
)

func TestEnvelopeShape(t *testing.T) {
	at := time.Date(2026, 9, 8, 9, 0, 0, 0, time.FixedZone("BRT", -3*3600))
	m, _ := money.Parse("25.00", money.BRL)
	before, _ := money.Parse("1000.00", money.BRL)
	after, _ := money.Parse("975.00", money.BRL)
	walletID, txID := uuid.New(), uuid.New()
	e := events.NewWalletBalanceChanged(events.Meta{CorrelationID: "corr-1", CausationID: "msg-1"}, at, events.WalletBalanceChangedData{
		WalletID: walletID, TransactionID: txID, Direction: "DEBIT", Money: m, BalanceBefore: before, BalanceAfter: after, WalletVersion: 2,
	})
	raw, err := e.Payload()
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"eventId", "eventType", "aggregateId", "correlationId", "causationId", "occurredAt", "version", "data"} {
		if _, ok := got[k]; !ok {
			t.Errorf("missing %s in %s", k, raw)
		}
	}
	if got["eventType"] != events.TypeWalletBalanceChanged || got["version"].(float64) != 1 {
		t.Errorf("type/version set by constructor: %v %v", got["eventType"], got["version"])
	}
	if got["occurredAt"] != "2026-09-08T12:00:00Z" {
		t.Errorf("occurredAt must be UTC RFC 3339: %v", got["occurredAt"])
	}
	data := got["data"].(map[string]any)
	for _, k := range []string{"walletId", "transactionId", "direction", "money", "balanceBefore", "balanceAfter", "walletVersion"} {
		if _, ok := data[k]; !ok {
			t.Errorf("missing data.%s", k)
		}
	}
	if data["money"].(map[string]any)["amount"] != "25.00" {
		t.Errorf("money must be a decimal string: %v", data["money"])
	}
	if e.PartitionKey() != walletID.String() || e.AggregateID() != walletID.String() {
		t.Error("partition key / aggregate id")
	}
	// The snapshot is stable: serializing again yields identical bytes.
	again, _ := e.Payload()
	if string(again) != string(raw) {
		t.Error("payload is not stable")
	}
	noCause := events.NewWalletBalanceChanged(events.Meta{CorrelationID: "c"}, at, events.WalletBalanceChangedData{
		WalletID: walletID, TransactionID: txID, Direction: "DEBIT", Money: m, BalanceBefore: before, BalanceAfter: after, WalletVersion: 2,
	})
	p, _ := noCause.Payload()
	var x map[string]any
	_ = json.Unmarshal(p, &x)
	if _, ok := x["causationId"]; ok {
		t.Error("causationId is optional")
	}
}
