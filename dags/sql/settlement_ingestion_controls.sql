-- Apply after rekon.sql. Additive migration; does not delete existing settlement data.
BEGIN;
ALTER TABLE sw_rekon.acqpwc_file_log_tt
    ADD COLUMN IF NOT EXISTS contentsha256 VARCHAR(64),
    ADD COLUMN IF NOT EXISTS fileversion INTEGER NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS isactive BOOLEAN NOT NULL DEFAULT TRUE,
    ADD COLUMN IF NOT EXISTS replacesfileid BIGINT REFERENCES sw_rekon.acqpwc_file_log_tt(fileid);

-- Existing duplicate active filenames must be reviewed; never silently delete them.
CREATE UNIQUE INDEX IF NOT EXISTS uq_acqpwc_active_d4_filename
    ON sw_rekon.acqpwc_file_log_tt (lower(filename))
    WHERE transactiontype = 'D4' AND isactive;
CREATE UNIQUE INDEX IF NOT EXISTS uq_acqpwc_d4_version
    ON sw_rekon.acqpwc_file_log_tt (lower(filename), fileversion)
    WHERE transactiontype = 'D4';

CREATE TABLE IF NOT EXISTS sw_rekon.acqpwc_reprocess_request_tt (
    requestid BIGSERIAL PRIMARY KEY,
    filename VARCHAR(100) NOT NULL,
    previoussha256 VARCHAR(64) NOT NULL CHECK (previoussha256 ~ '^[0-9a-f]{64}$'),
    expectedsha256 VARCHAR(64) NOT NULL CHECK (expectedsha256 ~ '^[0-9a-f]{64}$'),
    approvedby VARCHAR(100) NOT NULL CHECK (length(trim(approvedby)) > 0),
    reason TEXT NOT NULL CHECK (length(trim(reason)) > 0),
    approvedat TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(16) NOT NULL DEFAULT 'APPROVED' CHECK (status IN ('APPROVED', 'APPLIED', 'CANCELLED')),
    appliedat TIMESTAMPTZ,
    oldfileid BIGINT REFERENCES sw_rekon.acqpwc_file_log_tt(fileid),
    newfileid BIGINT REFERENCES sw_rekon.acqpwc_file_log_tt(fileid),
    CHECK (previoussha256 <> expectedsha256)
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_acqpwc_reprocess_approved
    ON sw_rekon.acqpwc_reprocess_request_tt(lower(filename), previoussha256, expectedsha256)
    WHERE status = 'APPROVED';

CREATE TABLE IF NOT EXISTS sw_rekon.acqpwc_ingestion_audit_tt (
    auditid BIGSERIAL PRIMARY KEY,
    runid TEXT NOT NULL,
    filename VARCHAR(255) NOT NULL,
    zipfilename VARCHAR(255),
    contentsha256 VARCHAR(64),
    status VARCHAR(32) NOT NULL CHECK (status IN
        ('WAITING_READY', 'RECEIVED', 'VERIFIED', 'VALIDATED', 'LOADED',
         'ALREADY_PROCESSED', 'QUARANTINED', 'QUARANTINE_FAILED')),
    stage VARCHAR(40) NOT NULL,
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    fileid BIGINT REFERENCES sw_rekon.acqpwc_file_log_tt(fileid),
    createdat TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS ix_acqpwc_audit_file_run
    ON sw_rekon.acqpwc_ingestion_audit_tt(filename, runid, createdat);
COMMIT;
