BEGIN;

CREATE SCHEMA IF NOT EXISTS sw_rekon;

CREATE TABLE sw_rekon.acqpwc_file_log_tt (
    fileid BIGSERIAL PRIMARY KEY,
    filepath VARCHAR(100) NOT NULL,
    filename VARCHAR(100),
    transactiontype VARCHAR(20),
    insertdate TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    insetby VARCHAR(100),
    issuername VARCHAR(120) DEFAULT 'PRIMA',
    filedate VARCHAR(6)
);

CREATE TABLE sw_rekon.acqpwc_d4summary_tt (
    summaryid BIGSERIAL PRIMARY KEY,
    fileid BIGINT NOT NULL,
    institutionid INTEGER,
    issuer VARCHAR(100),
    period DATE,
    frekuensi INTEGER,
    totalamount NUMERIC(19,2),

    CONSTRAINT fk_acqpwc_d4summary_file
        FOREIGN KEY (fileid)
        REFERENCES sw_rekon.acqpwc_file_log_tt (fileid)
);

CREATE TABLE sw_rekon.acqpwc_d4detail_tt (
    detailid BIGSERIAL PRIMARY KEY,
    summaryid BIGINT NOT NULL,
    transactiondate DATE,
    transactiontime TIME,
    transactioncode VARCHAR(8),
    transactiontype VARCHAR(8),
    cardno VARCHAR(60),
    transactionamount NUMERIC(19,2),
    interchangefee NUMERIC(19,2),
    authcode VARCHAR(10),
    traceno VARCHAR(10),
    refno VARCHAR(100),
    termid VARCHAR(50),
    merchantid VARCHAR(60),
    merchantname VARCHAR(250),

    CONSTRAINT fk_acqpwc_d4detail_summary
        FOREIGN KEY (summaryid)
        REFERENCES sw_rekon.acqpwc_d4summary_tt (summaryid)
);

COMMIT;
