-- Local synthetic enrichment database; run with psql -v ON_ERROR_STOP=1.
SELECT 'CREATE DATABASE db_acq_psql OWNER airflow'
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'db_acq_psql')
\gexec
\connect db_acq_psql
BEGIN;
CREATE TABLE IF NOT EXISTS merchant_tm (
    mid VARCHAR(64) PRIMARY KEY,
    merchant_criteria VARCHAR(3) NOT NULL
);
CREATE TABLE IF NOT EXISTS qris_transaction_log_tt (
    rrn VARCHAR(32) PRIMARY KEY,
    mid VARCHAR(64) REFERENCES merchant_tm(mid),
    from_account_type VARCHAR(2),
    to_account_type VARCHAR(2),
    mpan VARCHAR(19),
    merchant_type VARCHAR(4),
    invoice_number VARCHAR(20),
    fee NUMERIC(18, 0)
);
INSERT INTO merchant_tm VALUES ('DUMMYMERCHANT001', 'UMI')
ON CONFLICT (mid) DO NOTHING;
INSERT INTO qris_transaction_log_tt
SELECT rrn, 'DUMMYMERCHANT001', '10', '20', '9360002801111177646',
       '5411', rrn, 0
FROM (VALUES
    ('260911000001'), ('260911000002'), ('260911000003'), ('260911000005'),
    ('260912000001'), ('260912000002'), ('260912000003'), ('260912000005')
) seed(rrn)
ON CONFLICT (rrn) DO NOTHING;
-- Enrichment untuk fixture Way4 hari ini dan kemarin (seed 04).
INSERT INTO qris_transaction_log_tt
SELECT to_char(d.day, 'YYMMDD') || lpad(s.suffix::text, 6, '0'),
       'DUMMYMERCHANT001', '10', '20', '9360002801111177646',
       '5411', to_char(d.day, 'YYMMDD') || lpad(s.suffix::text, 6, '0'), 0
FROM (
    SELECT (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Jakarta')::date AS day
    UNION ALL
    SELECT (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Jakarta')::date - 1
) d
CROSS JOIN (VALUES (1), (2), (3), (5)) s(suffix)
ON CONFLICT (rrn) DO NOTHING;
-- Sinkronkan fixture enrichment lama ke MPAN merchant PWC lokal.
UPDATE qris_transaction_log_tt
SET mpan = '9360002801111177646'
WHERE mid = 'DUMMYMERCHANT001' AND mpan = '9360002810000000000';
COMMIT;
