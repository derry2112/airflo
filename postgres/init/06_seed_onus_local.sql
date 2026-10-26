-- Synthetic ONUS/Way4 data; safe to rerun. Run using psql -v ON_ERROR_STOP=1.
\connect db_rekon_uat
BEGIN;
SET LOCAL lock_timeout = '5s';
LOCK TABLE sw_replicate.on_doc_transaction IN SHARE ROW EXCLUSIVE MODE;
INSERT INTO sw_replicate.on_doc_transaction (
    docid, sourceregnum, period, periodtime, pan, transactiontype,
    stan, rrn, reversalseq, rc, connection_acq, connection_iss,
    institutionbranch_acq, transactionamount, transactioncurrency,
    merchantid, auth_code, add_data
)
SELECT 98000000000000000000::numeric + to_char(d.day, 'YYYYMMDD')::numeric * 100 + s.id,
       'ONUS_' || to_char(d.day, 'YYYYMMDD') || '_' || s.id,
       to_char(d.day, 'YYYYMMDD'), s.tm, '9360002810000000000', s.kind,
       lpad(s.id::text, 6, '0'),
       to_char(d.day, 'YYMMDD') || lpad(s.rrn_suffix::text, 6, '0'),
       0, s.rc, 'T', s.issuer, 'ONUS001', s.amount, '360',
       '111117764535', '900001', 'LOCAL_DUMMY_ONUS_V1'
FROM (
    SELECT (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Jakarta')::date AS day
    UNION SELECT (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Jakarta')::date - 1
    UNION SELECT DATE '2026-10-19'
) d
CROSS JOIN (VALUES
    (1, 910001, 'CH Payment', '0',  '110001', 10000.00, 'ACQQR'),
    (2, 910002, 'Credit',     '0',  '110002', 20000.00, 'a'),
    (3, 910003, 'CH Payment', '68', '110003', 30000.00, 'O'),
    (4, 910003, 'Retail',     '0',  '110004', 30000.00, 'O'),
    (5, 910005, 'Credit',     '82', '110005', 40000.00, 'ACQQR'),
    (6, 910005, 'Retail',     '0',  '110006', 40000.00, 'ACQQR')
) s(id, rrn_suffix, kind, rc, tm, amount, issuer)
WHERE NOT EXISTS (
    SELECT 1 FROM sw_replicate.on_doc_transaction existing
    WHERE existing.docid = 98000000000000000000::numeric
        + to_char(d.day, 'YYYYMMDD')::numeric * 100 + s.id
);
COMMIT;

\connect db_acq_psql
BEGIN;
INSERT INTO merchant_tm (mid, merchant_criteria) VALUES ('111117764535', 'UMI')
ON CONFLICT (mid) DO NOTHING;
INSERT INTO qris_transaction_log_tt (
    rrn, mid, from_account_type, to_account_type, mpan,
    merchant_type, invoice_number, fee
)
SELECT to_char(d.day, 'YYMMDD') || lpad(s.suffix::text, 6, '0'),
       '111117764535', '10', '20', '9360002801111177646', '5411',
       to_char(d.day, 'YYMMDD') || lpad(s.suffix::text, 6, '0'), 0
FROM (
    SELECT (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Jakarta')::date AS day
    UNION SELECT (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Jakarta')::date - 1
    UNION SELECT DATE '2026-10-19'
) d
CROSS JOIN (VALUES (910001), (910002), (910003), (910005)) s(suffix)
ON CONFLICT (rrn) DO NOTHING;
COMMIT;
