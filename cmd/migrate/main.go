// Command migrate applies or reverts the versioned SQL migrations.
//
//	migrate up            apply every pending migration
//	migrate down [N]      revert N migrations (default 1)
//	migrate down-all      revert everything
//	migrate version       print the current version
//	migrate force V       mark version V as clean (after a failed migration)
//
// The database URL comes from MIGRATIONS_DATABASE_URL (schema owner role).
package main

import (
	"errors"
	"fmt"
	"os"
	"strconv"

	"github.com/golang-migrate/migrate/v4"

	"github.com/julioCAlmeida/backend-challenge-go/internal/infra/postgres"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "migrate:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	url := os.Getenv("MIGRATIONS_DATABASE_URL")
	if url == "" {
		return errors.New("MIGRATIONS_DATABASE_URL is required")
	}
	if len(args) == 0 {
		return errors.New("usage: migrate up | down [N] | down-all | version | force V")
	}
	m, err := postgres.NewMigrator(url)
	if err != nil {
		return err
	}
	defer m.Close()

	switch args[0] {
	case "up":
		err = m.Up()
	case "down":
		n := 1
		if len(args) > 1 {
			if n, err = strconv.Atoi(args[1]); err != nil || n < 1 {
				return errors.New("down expects a positive number of steps")
			}
		}
		err = m.Steps(-n)
	case "down-all":
		err = m.Down()
	case "force":
		if len(args) < 2 {
			return errors.New("force expects a version")
		}
		v, convErr := strconv.Atoi(args[1])
		if convErr != nil {
			return convErr
		}
		err = m.Force(v)
	case "version":
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
	if err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return err
	}
	v, dirty, verr := m.Version()
	if errors.Is(verr, migrate.ErrNilVersion) {
		fmt.Println("version: none")
		return nil
	}
	if verr != nil {
		return verr
	}
	fmt.Printf("version: %d dirty: %v\n", v, dirty)
	return nil
}
