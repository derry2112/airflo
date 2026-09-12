-- Local synthetic fixtures for the Way4 QR query. Safe to rerun.
BEGIN;
SET LOCAL lock_timeout = '5s';
LOCK TABLE sw_replicate.on_doc_transaction IN SHARE ROW EXCLUSIVE MODE;
INSERT INTO sw_replicate.on_doc_transaction (
    docid, sourceregnum, period, periodtime, pan, transactiontype,
    stan, rrn, reversalseq, rc, connection_acq, connection_iss,
    institutionbranch_acq, transactionamount, transactioncurrency,
    merchantid, auth_code, add_data
)
SELECT
    99000000000000000000::numeric + to_char(d.day, 'YYYYMMDD')::numeric * 100 + s.id,
    'DUMMY_QR_' || to_char(d.day, 'YYYYMMDD') || '_' || s.id,
    to_char(d.day, 'YYYYMMDD'), s.tm, '9360002810000000000', s.kind,
    lpad(s.id::text, 6, '0'),
    to_char(d.day, 'YYMMDD') || lpad(s.rrn_suffix::text, 6, '0'),
    0, s.rc, 'T', 'ACQQR', 'DUMMYTERM001', s.amount, '360',
    'DUMMYMERCHANT001', '000001', 'LOCAL_DUMMY_WAY4_QR_V1'
FROM (
    SELECT (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Jakarta')::date AS day
    UNION ALL
    SELECT (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Jakarta')::date - 1
) d
CROSS JOIN (VALUES
    (1, 1, 'CH Payment', '0',  '100001', 10000.00),
    (2, 2, 'Credit',     '0',  '100002', 20000.00),
    (3, 3, 'CH Payment', '68', '100003', 30000.00),
    (4, 3, 'Retail',     '0',  '100004', 30000.00),
    (5, 5, 'Credit',     '82', '100005', 40000.00),
    (6, 5, 'Retail',     '0',  '100006', 40000.00)
) s(id, rrn_suffix, kind, rc, tm, amount)
WHERE NOT EXISTS (
    SELECT 1 FROM sw_replicate.on_doc_transaction existing
    WHERE existing.docid = 99000000000000000000::numeric
        + to_char(d.day, 'YYYYMMDD')::numeric * 100 + s.id
);
COMMIT;
