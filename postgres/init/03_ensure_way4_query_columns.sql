-- Ensure columns required by get_way4_data_new. Existing compatible columns and data are preserved.
BEGIN;
SET LOCAL lock_timeout = '5s';
CREATE SCHEMA IF NOT EXISTS sw_replicate;
CREATE TABLE IF NOT EXISTS sw_replicate.on_doc_transaction (
    merchantid VARCHAR(64),
    institutionbranch_acq VARCHAR(64),
    rrn VARCHAR(32),
    period VARCHAR(8),
    periodtime VARCHAR(6),
    transactionamount NUMERIC(24, 4),
    transactioncurrency VARCHAR(8),
    pan VARCHAR(32),
    rc VARCHAR(8),
    transactiontype VARCHAR(64),
    auth_code VARCHAR(32),
    reversalseq INTEGER,
    sourceregnum VARCHAR(64),
    stan VARCHAR(16),
    connection_acq VARCHAR(64),
    connection_iss VARCHAR(64)
);

ALTER TABLE sw_replicate.on_doc_transaction
    ADD COLUMN IF NOT EXISTS merchantid VARCHAR(64),
    ADD COLUMN IF NOT EXISTS institutionbranch_acq VARCHAR(64),
    ADD COLUMN IF NOT EXISTS rrn VARCHAR(32),
    ADD COLUMN IF NOT EXISTS period VARCHAR(8),
    ADD COLUMN IF NOT EXISTS periodtime VARCHAR(6),
    ADD COLUMN IF NOT EXISTS transactionamount NUMERIC(24, 4),
    ADD COLUMN IF NOT EXISTS transactioncurrency VARCHAR(8),
    ADD COLUMN IF NOT EXISTS pan VARCHAR(32),
    ADD COLUMN IF NOT EXISTS rc VARCHAR(8),
    ADD COLUMN IF NOT EXISTS transactiontype VARCHAR(64),
    ADD COLUMN IF NOT EXISTS auth_code VARCHAR(32),
    ADD COLUMN IF NOT EXISTS reversalseq INTEGER,
    ADD COLUMN IF NOT EXISTS sourceregnum VARCHAR(64),
    ADD COLUMN IF NOT EXISTS stan VARCHAR(16),
    ADD COLUMN IF NOT EXISTS connection_acq VARCHAR(64),
    ADD COLUMN IF NOT EXISTS connection_iss VARCHAR(64);
COMMIT;
