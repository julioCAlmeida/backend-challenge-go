DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'wallet_app') THEN
        REVOKE ALL ON wallets, wager_transactions, inbox_messages, outbox_events, wallet_ledger_entries FROM wallet_app;
        REVOKE USAGE ON SCHEMA public FROM wallet_app;
    END IF;
END;
$$;
