package postgres

import (
	"errors"
	"fmt"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5" // pgx5:// driver
	"github.com/golang-migrate/migrate/v4/source/iofs"

	"github.com/julioCAlmeida/backend-challenge-go/migrations"
)

// NewMigrator builds a golang-migrate instance over the embedded migrations.
// databaseURL is a postgres:// URL of a role that owns the schema.
func NewMigrator(databaseURL string) (*migrate.Migrate, error) {
	src, err := iofs.New(migrations.FS, ".")
	if err != nil {
		return nil, err
	}
	url := databaseURL
	for _, p := range []string{"postgres://", "postgresql://"} {
		if strings.HasPrefix(url, p) {
			url = "pgx5://" + strings.TrimPrefix(url, p)
		}
	}
	m, err := migrate.NewWithSourceInstance("iofs", src, url)
	if err != nil {
		return nil, fmt.Errorf("init migrations: %w", err)
	}
	return m, nil
}

// MigrateUp applies every pending migration.
func MigrateUp(databaseURL string) error {
	m, err := NewMigrator(databaseURL)
	if err != nil {
		return err
	}
	defer m.Close()
	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return err
	}
	return nil
}
