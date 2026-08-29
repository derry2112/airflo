CREATE SCHEMA IF NOT EXISTS sw_replicate;

CREATE TABLE IF NOT EXISTS sw_replicate.on_doc_transaction (
    docid                         NUMERIC(20, 0),
    sourceregnum                  VARCHAR(64),
    documenttype                 VARCHAR(16),
    period                        VARCHAR(8),
    periodtime                    VARCHAR(6),
    pan                           VARCHAR(32),
    transactiontype               VARCHAR(64),
    stan                          VARCHAR(16),
    rrn                           VARCHAR(32),
    transactionstatus             VARCHAR(32),
    reversalseq                    INTEGER,
    settlementdate                VARCHAR(8),
    rc                            VARCHAR(8),
    connection_acq                VARCHAR(64),
    connection_iss                VARCHAR(64),
    connection_dest               VARCHAR(64),
    institution_acq               VARCHAR(64),
    institution_iss               VARCHAR(64),
    institution_dest              VARCHAR(64),
    institutionbranch_acq         VARCHAR(64),
    institutionbranch_iss         VARCHAR(64),
    institutionbranch_dest        VARCHAR(64),
    countrycode                   VARCHAR(8),
    countrycodenumeric            VARCHAR(8),
    transactionamount             NUMERIC(24, 4),
    transactioncurrency           VARCHAR(8),
    reconamount                   NUMERIC(24, 4),
    reconcurrency                 VARCHAR(8),
    settlementamount              NUMERIC(24, 4),
    settlementcurrency            VARCHAR(8),
    originalamount                NUMERIC(24, 4),
    originalcurrency              VARCHAR(8),
    reversalamount                NUMERIC(24, 4),
    accountid_iss                 VARCHAR(64),
    accountid_dest                VARCHAR(64),
    customerid                    VARCHAR(64),
    referencenumber               VARCHAR(64),
    transactionchargeamount       NUMERIC(24, 4),
    transactionchargecurrency     VARCHAR(8),
    settlementchargeamount        NUMERIC(24, 4),
    settlementchargecurrency      VARCHAR(8),
    merchantid                    VARCHAR(64),
    auth_code                     VARCHAR(32),
    org_chrg_amt                  NUMERIC(24, 4),
    addl_chrg_amt                 NUMERIC(24, 4),
    add_data                      TEXT
);

CREATE INDEX IF NOT EXISTS idx_on_doc_transaction_connection_acq
    ON sw_replicate.on_doc_transaction (connection_acq)
    WHERE connection_acq IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_on_doc_transaction_period
    ON sw_replicate.on_doc_transaction (period, periodtime);

CREATE INDEX IF NOT EXISTS idx_on_doc_transaction_rrn
    ON sw_replicate.on_doc_transaction (rrn);
