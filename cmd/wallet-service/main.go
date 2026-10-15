// Command wallet-service runs the HTTP API, the SQS consumer, the outbox
// publisher and the pending-reference worker. Several instances can run
// against the same PostgreSQL and SQS.
package main

import (
	"fmt"
	"os"

	"github.com/julioCAlmeida/backend-challenge-go/internal/bootstrap"
	"github.com/julioCAlmeida/backend-challenge-go/internal/config"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid configuration: %v\n", err)
		os.Exit(2)
	}
	// Run blocks until SIGINT/SIGTERM, then runs the OnStop hooks within
	// SHUTDOWN_TIMEOUT.
	bootstrap.New(cfg).Run()
}
