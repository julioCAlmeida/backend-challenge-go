-- Wallets, wager transactions, append-only ledger, inbox and outbox.
-- Money is stored as BIGINT minor units (cents) + ISO 4217 code, never as float.

CREATE TABLE wallets (
    id            uuid        PRIMARY KEY,
    player_id     uuid        NOT NULL,
    currency      char(3)     NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    balance_minor bigint      NOT NULL CONSTRAINT wallets_balance_non_negative CHECK (balance_minor >= 0),
    version       bigint      NOT NULL CHECK (version >= 1),
    created_at    timestamptz NOT NULL,
    updated_at    timestamptz NOT NULL,
    CONSTRAINT wallets_player_currency_uk UNIQUE (player_id, currency),
    CONSTRAINT wallets_id_currency_uk UNIQUE (id, currency)
);

CREATE TABLE wager_transactions (
    id                                uuid        PRIMARY KEY,
    origin                            text        NOT NULL CHECK (origin IN ('INTERNAL', 'EXTERNAL')),
    kind                              text        NOT NULL CHECK (kind IN ('OPENING', 'BET', 'WIN', 'LOSS', 'REFUND', 'ROLLBACK')),
    status                            text        NOT NULL CHECK (status IN ('PENDING', 'PENDING_REFERENCE', 'PROCESSED', 'REJECTED', 'FAILED')),
    provider_id                       text,
    external_transaction_id           text,
    idempotency_key                   text,
    payload_hash                      char(64),
    wallet_id                         uuid        NOT NULL REFERENCES wallets (id),
    player_id                         uuid        NOT NULL,
    round_id                          text,
    game_id                           text,
    amount_minor                      bigint      NOT NULL,
    currency                          char(3)     NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    reference_external_transaction_id text,
    reference_transaction_id          uuid        REFERENCES wager_transactions (id),
    failure_code                      text,
    result_balance_minor              bigint,
    reference_attempts                integer     NOT NULL DEFAULT 0 CHECK (reference_attempts >= 0),
    next_attempt_at                   timestamptz,
    correlation_id                    text,
    causation_id                      text,
    created_at                        timestamptz NOT NULL,
    updated_at                        timestamptz NOT NULL,
    completed_at                      timestamptz,

    -- Internal (OPENING) and external operations have different shapes.
    CONSTRAINT wager_tx_origin_shape CHECK (
        (origin = 'INTERNAL' AND kind = 'OPENING'
            AND provider_id IS NULL AND external_transaction_id IS NULL AND idempotency_key IS NULL
            AND payload_hash IS NULL AND round_id IS NULL AND game_id IS NULL
            AND reference_external_transaction_id IS NULL AND reference_transaction_id IS NULL)
        OR
        (origin = 'EXTERNAL' AND kind <> 'OPENING'
            AND provider_id IS NOT NULL AND external_transaction_id IS NOT NULL AND idempotency_key IS NOT NULL
            AND payload_hash IS NOT NULL AND round_id IS NOT NULL AND game_id IS NOT NULL)
    ),
    -- Zero only for LOSS; every other kind moves a positive amount.
    CONSTRAINT wager_tx_amount_policy CHECK (
        (kind = 'LOSS' AND amount_minor = 0) OR (kind <> 'LOSS' AND amount_minor > 0)
    ),
    CONSTRAINT wager_tx_reference_policy CHECK (
        (kind IN ('REFUND', 'ROLLBACK') AND reference_external_transaction_id IS NOT NULL)
        OR (kind IN ('OPENING', 'BET', 'LOSS') AND reference_external_transaction_id IS NULL)
        OR kind = 'WIN'
    ),
    CONSTRAINT wager_tx_status_shape CHECK (
        ((status IN ('REJECTED', 'FAILED')) = (failure_code IS NOT NULL))
        AND ((status IN ('PROCESSED', 'REJECTED', 'FAILED')) = (completed_at IS NOT NULL))
        AND (status <> 'PROCESSED' OR result_balance_minor IS NOT NULL)
        AND (status <> 'PENDING_REFERENCE' OR next_attempt_at IS NOT NULL)
    ),
    CONSTRAINT wager_tx_result_balance_non_negative CHECK (result_balance_minor IS NULL OR result_balance_minor >= 0),
    -- Persistent idempotency, scoped by provider (NULLs never collide, so internal rows are unaffected).
    CONSTRAINT wager_tx_provider_idempotency_key_uk UNIQUE (provider_id, idempotency_key),
    CONSTRAINT wager_tx_provider_external_id_uk UNIQUE (provider_id, external_transaction_id)
);

-- At most one opening credit per wallet.
CREATE UNIQUE INDEX wager_tx_single_opening_uk ON wager_transactions (wallet_id) WHERE kind = 'OPENING';

-- A transaction receives at most one successful reversal (REFUND or ROLLBACK).
CREATE UNIQUE INDEX wager_tx_single_reversal_uk ON wager_transactions (reference_transaction_id)
    WHERE status = 'PROCESSED' AND kind IN ('REFUND', 'ROLLBACK');

-- Durable resumption of pending work.
CREATE INDEX wager_tx_pending_idx ON wager_transactions (next_attempt_at)
    WHERE status IN ('PENDING', 'PENDING_REFERENCE');
CREATE INDEX wager_tx_waiting_reference_idx ON wager_transactions (provider_id, reference_external_transaction_id)
    WHERE status = 'PENDING_REFERENCE';
CREATE INDEX wager_tx_wallet_idx ON wager_transactions (wallet_id, created_at);

CREATE TABLE wallet_ledger_entries (
    id                   uuid        PRIMARY KEY,
    seq                  bigint      GENERATED ALWAYS AS IDENTITY,
    wallet_id            uuid        NOT NULL,
    transaction_id       uuid        NOT NULL REFERENCES wager_transactions (id),
    direction            text        NOT NULL CHECK (direction IN ('DEBIT', 'CREDIT')),
    amount_minor         bigint      NOT NULL CHECK (amount_minor > 0),
    currency             char(3)     NOT NULL,
    balance_before_minor bigint      NOT NULL CHECK (balance_before_minor >= 0),
    balance_after_minor  bigint      NOT NULL CHECK (balance_after_minor >= 0),
    wallet_version       bigint      NOT NULL CHECK (wallet_version >= 1),
    created_at           timestamptz NOT NULL,
    CONSTRAINT ledger_wallet_currency_fk FOREIGN KEY (wallet_id, currency) REFERENCES wallets (id, currency),
    CONSTRAINT ledger_wallet_transaction_uk UNIQUE (wallet_id, transaction_id),
    -- One entry per wallet version: two writers can never both apply "version + 1".
    CONSTRAINT ledger_wallet_version_uk UNIQUE (wallet_id, wallet_version),
    CONSTRAINT ledger_seq_uk UNIQUE (seq),
    CONSTRAINT ledger_balance_arithmetic CHECK (
        (direction = 'CREDIT' AND balance_after_minor = balance_before_minor + amount_minor)
        OR (direction = 'DEBIT' AND balance_after_minor = balance_before_minor - amount_minor)
    )
);
CREATE INDEX ledger_wallet_seq_idx ON wallet_ledger_entries (wallet_id, seq);

CREATE TABLE inbox_messages (
    consumer_name  text        NOT NULL,
    message_id     text        NOT NULL,
    payload_hash   char(64)    NOT NULL,
    transaction_id uuid        REFERENCES wager_transactions (id),
    outcome        text,
    received_at    timestamptz NOT NULL,
    completed_at   timestamptz,
    PRIMARY KEY (consumer_name, message_id)
);

CREATE TABLE outbox_events (
    event_id        uuid        PRIMARY KEY,
    aggregate_type  text        NOT NULL,
    aggregate_id    text        NOT NULL,
    partition_key   text        NOT NULL,
    event_type      text        NOT NULL,
    -- json (not jsonb) keeps the exact serialized snapshot that is published.
    payload         json        NOT NULL,
    occurred_at     timestamptz NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now(),
    attempts        integer     NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at timestamptz NOT NULL,
    locked_by       text,
    locked_until    timestamptz,
    published_at    timestamptz,
    last_error      text
);
CREATE INDEX outbox_unpublished_idx ON outbox_events (next_attempt_at) WHERE published_at IS NULL;

-------------------------------------------------------------------------------
-- Protection triggers
-------------------------------------------------------------------------------

-- Ledger: append-only.
CREATE FUNCTION ledger_reject_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'wallet_ledger_entries is append-only (% rejected)', TG_OP
        USING ERRCODE = 'insufficient_privilege';
END;
$$;
CREATE TRIGGER ledger_no_update_delete BEFORE UPDATE OR DELETE ON wallet_ledger_entries
    FOR EACH ROW EXECUTE FUNCTION ledger_reject_mutation();
CREATE TRIGGER ledger_no_truncate BEFORE TRUNCATE ON wallet_ledger_entries
    FOR EACH STATEMENT EXECUTE FUNCTION ledger_reject_mutation();

-- Wallets: never deleted; identity immutable; balance changes require version + 1
-- and a ledger entry for that exact version and balance (checked at commit).
CREATE FUNCTION wallets_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'wallets cannot be deleted' USING ERRCODE = 'insufficient_privilege';
    END IF;
    IF NEW.id <> OLD.id OR NEW.player_id <> OLD.player_id OR NEW.currency <> OLD.currency
        OR NEW.created_at <> OLD.created_at THEN
        RAISE EXCEPTION 'wallet identity is immutable' USING ERRCODE = 'check_violation';
    END IF;
    IF NEW.balance_minor = OLD.balance_minor AND NEW.version <> OLD.version THEN
        RAISE EXCEPTION 'wallet version changes only with the balance' USING ERRCODE = 'check_violation';
    END IF;
    IF NEW.balance_minor <> OLD.balance_minor AND NEW.version <> OLD.version + 1 THEN
        RAISE EXCEPTION 'wallet version must be incremented by exactly one' USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER wallets_guard BEFORE UPDATE OR DELETE ON wallets
    FOR EACH ROW EXECUTE FUNCTION wallets_guard();

CREATE FUNCTION wallets_require_ledger() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    e record;
    current_balance bigint;
    current_version bigint;
BEGIN
    -- Evaluate against the row as it is at commit time.
    SELECT balance_minor, version INTO current_balance, current_version FROM wallets WHERE id = NEW.id;
    IF TG_OP = 'INSERT' THEN
        IF current_version = 1 AND current_balance = 0 THEN
            RETURN NULL;
        END IF;
    ELSIF NEW.version = OLD.version THEN
        RETURN NULL;
    END IF;
    SELECT balance_after_minor INTO e FROM wallet_ledger_entries
        WHERE wallet_id = NEW.id AND wallet_version = current_version;
    IF NOT FOUND OR e.balance_after_minor <> current_balance THEN
        RAISE EXCEPTION 'wallet % balance % (version %) has no matching ledger entry', NEW.id, current_balance, current_version
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER wallets_balance_matches_ledger AFTER INSERT OR UPDATE ON wallets
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION wallets_require_ledger();

-- Wager transactions: never deleted; business fields immutable; terminal states final.
CREATE FUNCTION wager_tx_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'wager transactions cannot be deleted' USING ERRCODE = 'insufficient_privilege';
    END IF;
    IF OLD.status IN ('PROCESSED', 'REJECTED', 'FAILED') THEN
        RAISE EXCEPTION 'wager transaction % is terminal (%)', OLD.id, OLD.status USING ERRCODE = 'check_violation';
    END IF;
    IF NEW.id <> OLD.id OR NEW.origin <> OLD.origin OR NEW.kind <> OLD.kind
        OR NEW.provider_id IS DISTINCT FROM OLD.provider_id
        OR NEW.external_transaction_id IS DISTINCT FROM OLD.external_transaction_id
        OR NEW.idempotency_key IS DISTINCT FROM OLD.idempotency_key
        OR NEW.payload_hash IS DISTINCT FROM OLD.payload_hash
        OR NEW.wallet_id <> OLD.wallet_id OR NEW.player_id <> OLD.player_id
        OR NEW.round_id IS DISTINCT FROM OLD.round_id OR NEW.game_id IS DISTINCT FROM OLD.game_id
        OR NEW.amount_minor <> OLD.amount_minor OR NEW.currency <> OLD.currency
        OR NEW.reference_external_transaction_id IS DISTINCT FROM OLD.reference_external_transaction_id
        OR NEW.created_at <> OLD.created_at THEN
        RAISE EXCEPTION 'wager transaction business fields are immutable' USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER wager_tx_guard BEFORE UPDATE OR DELETE ON wager_transactions
    FOR EACH ROW EXECUTE FUNCTION wager_tx_guard();

-- Outbox: the event snapshot is immutable and a published event stays published.
CREATE FUNCTION outbox_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.event_id <> OLD.event_id OR NEW.event_type <> OLD.event_type OR NEW.payload::text <> OLD.payload::text
        OR NEW.aggregate_id <> OLD.aggregate_id OR NEW.aggregate_type <> OLD.aggregate_type
        OR NEW.partition_key <> OLD.partition_key OR NEW.occurred_at <> OLD.occurred_at THEN
        RAISE EXCEPTION 'outbox event snapshot is immutable' USING ERRCODE = 'check_violation';
    END IF;
    IF OLD.published_at IS NOT NULL AND NEW.published_at IS DISTINCT FROM OLD.published_at THEN
        RAISE EXCEPTION 'outbox event % is already published', OLD.event_id USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER outbox_guard BEFORE UPDATE ON outbox_events
    FOR EACH ROW EXECUTE FUNCTION outbox_guard();

-- Inbox: identity and hash are immutable.
CREATE FUNCTION inbox_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.consumer_name <> OLD.consumer_name OR NEW.message_id <> OLD.message_id
        OR NEW.payload_hash <> OLD.payload_hash OR NEW.received_at <> OLD.received_at THEN
        RAISE EXCEPTION 'inbox message identity is immutable' USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER inbox_guard BEFORE UPDATE ON inbox_messages
    FOR EACH ROW EXECUTE FUNCTION inbox_guard();
