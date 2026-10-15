package contract_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/julioCAlmeida/backend-challenge-go/internal/contract"
	"github.com/julioCAlmeida/backend-challenge-go/internal/domain/money"
	"github.com/julioCAlmeida/backend-challenge-go/internal/domain/wager"
)

const httpBody = `{
  "providerId": "provider-a",
  "externalTransactionId": "transaction-123",
  "playerId": "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
  "walletId": "0192f291-27dd-7d3f-8071-5f8685deef37",
  "roundId": "round-987",
  "gameId": "fortune-chimp",
  "kind": "BET",
  "money": { "amount": "25.00", "currency": "BRL" }
}`

const sqsBody = `{
  "messageId": "msg-123",
  "type": "WagerTransactionRequested",
  "occurredAt": "2026-09-08T12:00:00.000Z",
  "data": {
    "providerId": "provider-a",
    "externalTransactionId": "transaction-123",
    "idempotencyKey": "provider-a:transaction-123",
    "playerId": "0192F28F-5DC0-7D58-BDB2-814AD6A0F4A1",
    "walletId": "0192f291-27dd-7d3f-8071-5f8685deef37",
    "roundId": "round-987",
    "gameId": "fortune-chimp",
    "kind": "BET",
    "money": { "amount": "25.00", "currency": "BRL" }
  }
}`

func TestHTTPAndSQSProduceTheSameHash(t *testing.T) {
	var f contract.WagerFields
	if err := contract.DecodeStrict(strings.NewReader(httpBody), &f); err != nil {
		t.Fatal(err)
	}
	httpReq, err := f.ToRequest("provider-a:transaction-123")
	if err != nil {
		t.Fatal(err)
	}
	_, sqsReq, inboxHash, err := contract.ParseWagerMessage([]byte(sqsBody))
	if err != nil {
		t.Fatal(err)
	}
	if httpReq.PayloadHash() != sqsReq.PayloadHash() {
		t.Fatal("HTTP and SQS must hash identically (UUID case is normalized)")
	}
	if len(inboxHash) != 64 {
		t.Fatalf("inbox hash %q", inboxHash)
	}
	// occurredAt is transport metadata: changing it keeps the inbox hash.
	_, _, h2, err := contract.ParseWagerMessage([]byte(strings.Replace(sqsBody, "12:00:00.000Z", "12:00:05.000Z", 1)))
	if err != nil || h2 != inboxHash {
		t.Fatalf("occurredAt changed the inbox hash: %v", err)
	}
}

func TestContractRejections(t *testing.T) {
	cases := map[string]string{
		"numeric amount":     strings.Replace(httpBody, `"25.00"`, `25.00`, 1),
		"scientific":         strings.Replace(httpBody, `"25.00"`, `"2.5e1"`, 1),
		"negative":           strings.Replace(httpBody, `"25.00"`, `"-25.00"`, 1),
		"scale":              strings.Replace(httpBody, `"25.00"`, `"25.001"`, 1),
		"NaN":                strings.Replace(httpBody, `"25.00"`, `"NaN"`, 1),
		"Infinity":           strings.Replace(httpBody, `"25.00"`, `"Infinity"`, 1),
		"empty":              strings.Replace(httpBody, `"25.00"`, `""`, 1),
		"OPENING":            strings.Replace(httpBody, `"BET"`, `"OPENING"`, 1),
		"unknown field":      strings.Replace(httpBody, `"kind"`, `"extra": 1, "kind"`, 1),
		"bad uuid":           strings.Replace(httpBody, `0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1`, `abc`, 1),
		"missing money":      strings.Replace(httpBody, `"money": { "amount": "25.00", "currency": "BRL" }`, `"gameId2": "x"`, 1),
		"lowercase currency": strings.Replace(httpBody, `"BRL"`, `"brl"`, 1),
	}
	for name, body := range cases {
		var f contract.WagerFields
		err := contract.DecodeStrict(strings.NewReader(body), &f)
		if err == nil {
			_, err = f.ToRequest("k")
		}
		if err == nil {
			t.Errorf("%s accepted", name)
		}
	}
	var f contract.WagerFields
	_ = contract.DecodeStrict(strings.NewReader(strings.Replace(httpBody, `"BET"`, `"OPENING"`, 1)), &f)
	if _, err := f.ToRequest("k"); !errors.Is(err, wager.ErrKindNotAllowed) {
		t.Errorf("OPENING error = %v", err)
	}
	f = contract.WagerFields{}
	_ = contract.DecodeStrict(strings.NewReader(strings.Replace(httpBody, `"25.00"`, `"-1.00"`, 1)), &f)
	if _, err := f.ToRequest("k"); !errors.Is(err, money.ErrNegativeAmount) {
		t.Errorf("negative error = %v", err)
	}
}

func TestMessageEnvelopeValidation(t *testing.T) {
	for name, body := range map[string]string{
		"type":       strings.Replace(sqsBody, "WagerTransactionRequested", "Other", 1),
		"messageId":  strings.Replace(sqsBody, `"msg-123"`, `""`, 1),
		"occurredAt": strings.Replace(sqsBody, "2026-09-08T12:00:00.000Z", "yesterday", 1),
		"key":        strings.Replace(sqsBody, `"provider-a:transaction-123"`, `""`, 1),
		"not json":   "{",
		"trailing":   sqsBody + "{}",
	} {
		if _, _, _, err := contract.ParseWagerMessage([]byte(body)); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
}
