package bootstrap_test

import (
	"testing"

	"go.uber.org/fx"

	"github.com/julioCAlmeida/backend-challenge-go/internal/bootstrap"
	"github.com/julioCAlmeida/backend-challenge-go/internal/config"
)

// TestCompositionIsValid checks the Fx dependency graph without starting it
// (no infrastructure needed). The start/stop behavior is covered by the
// integration test TestFxLifecycleStartsAndReleasesResources.
func TestCompositionIsValid(t *testing.T) {
	cfg, err := config.LoadFrom(func(k string) string {
		return map[string]string{
			"DATABASE_URL":    "postgres://u:p@localhost:5432/db",
			"OIDC_ISSUER_URL": "http://localhost:8180/realms/wagering",
			"INSTANCE_ID":     "validate",
		}[k]
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := fx.ValidateApp(bootstrap.Module(cfg)); err != nil {
		t.Fatalf("invalid Fx graph: %v", err)
	}
}
