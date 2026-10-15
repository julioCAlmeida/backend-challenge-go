package config_test

import (
	"testing"

	"github.com/julioCAlmeida/backend-challenge-go/internal/config"
)

func envOf(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

func base() map[string]string {
	return map[string]string{
		"DATABASE_URL":    "postgres://wallet_app:x@localhost:5432/wagering",
		"OIDC_ISSUER_URL": "http://keycloak:8080/realms/wagering",
		"INSTANCE_ID":     "test",
	}
}

func TestLoadDefaults(t *testing.T) {
	c, err := config.LoadFrom(envOf(base()))
	if err != nil {
		t.Fatal(err)
	}
	if c.OIDCJWKSURL != "http://keycloak:8080/realms/wagering/protocol/openid-connect/certs" {
		t.Errorf("jwks %s", c.OIDCJWKSURL)
	}
	if c.WagerQueue != "wager-transactions.fifo" || c.WagerDLQ != "wager-transactions-dlq.fifo" {
		t.Errorf("queues %s %s", c.WagerQueue, c.WagerDLQ)
	}
}

func TestLoadRejectsInvalidConfiguration(t *testing.T) {
	cases := map[string]map[string]string{
		"missing db":        {"DATABASE_URL": ""},
		"missing issuer":    {"OIDC_ISSUER_URL": ""},
		"relative issuer":   {"OIDC_ISSUER_URL": "keycloak/realms/x"},
		"bad duration":      {"SHUTDOWN_TIMEOUT": "soon"},
		"processing >= vis": {"SQS_PROCESSING_TIMEOUT": "40s", "SQS_VISIBILITY_TIMEOUT": "30s"},
		"wait time":         {"SQS_WAIT_TIME": "30s"},
		"workers":           {"SQS_CONSUMER_WORKERS": "0"},
		"bool":              {"OUTBOX_ENABLED": "maybe"},
	}
	for name, override := range cases {
		m := base()
		for k, v := range override {
			m[k] = v
		}
		if _, err := config.LoadFrom(envOf(m)); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
}
