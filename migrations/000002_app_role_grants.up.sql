-- The application connects with the least-privileged role `wallet_app`
-- (created by deploy/postgres/init-roles.sh). It cannot UPDATE/DELETE the ledger
-- nor DELETE wallets/transactions, independently of the protection triggers.
-- Migrations run with the schema owner.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'wallet_app') THEN
        GRANT USAGE ON SCHEMA public TO wallet_app;
        GRANT SELECT, INSERT, UPDATE ON wallets, wager_transactions, inbox_messages, outbox_events TO wallet_app;
        GRANT SELECT, INSERT ON wallet_ledger_entries TO wallet_app;
    ELSE
        RAISE NOTICE 'role wallet_app not found: grants skipped';
    END IF;
END;
$$;
