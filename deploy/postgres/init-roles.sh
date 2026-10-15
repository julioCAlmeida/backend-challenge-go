#!/bin/sh
# Runs once, when the PostgreSQL volume is initialized.
# Creates the least-privileged application role. The schema owner
# (POSTGRES_USER) runs the migrations; the service connects as wallet_app.
set -eu
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-SQL
	DO \$\$
	BEGIN
	    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'wallet_app') THEN
	        CREATE ROLE wallet_app LOGIN PASSWORD '${WALLET_APP_PASSWORD}';
	    END IF;
	END
	\$\$;
	REVOKE CREATE ON SCHEMA public FROM PUBLIC;
SQL
