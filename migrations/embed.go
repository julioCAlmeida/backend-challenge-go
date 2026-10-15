// Package migrations embeds the versioned SQL migrations (golang-migrate format).
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
