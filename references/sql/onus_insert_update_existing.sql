/*
DATA UJI UNTUK FLOW ONUS DI dag_postingfile.py

Tujuan: menyediakan 2 transaksi yang lolos SELECT _get_way4_data_new,
lalu memakai UPDATE period yang sama setiap kali ingin generate ulang.

Jalankan langkah 1 di database Way4 (connection config['db_way4']).
Jalankan langkah 2 di database enrichment (connection config['db_acq']).
Kedua database berbeda: jangan menjalankan seluruh file pada satu koneksi.

MPAN 9360002801111177646 dan MID 111117764535 adalah contoh yang sudah
berhasil di lingkungan lokal. Di existing, ganti dengan MPAN merchant PWC
yang benar-benar tersedia di Oracle dan MID yang sesuai.
Kolom/constraint wajib tambahan pada schema existing perlu disesuaikan.
Tidak ada perubahan pada DAG atau hook existing.
*/

-- =====================================================================
-- 1. DATABASE WAY4: INSERT SEKALI, UPDATE PERIOD SETIAP AKAN UJI
-- =====================================================================
BEGIN;

-- INSERT data CH Payment (debit) dan Credit (kredit), RC 0.
-- RC 0 dipakai agar contoh ini tidak membutuhkan transaksi Retail pendukung.
-- RRN dan docid khusus contoh; pastikan belum dipakai transaksi lain.
INSERT INTO sw_replicate.on_doc_transaction (
    docid, sourceregnum, period, periodtime, pan, transactiontype,
    stan, rrn, reversalseq, rc, connection_acq, connection_iss,
    institutionbranch_acq, transactionamount, transactioncurrency,
    merchantid, auth_code
)
SELECT * FROM (VALUES
    (97000000002026101901::numeric, 'ONUS_TEST_000001', '20261019', '120001',
     '9360002810000000000', 'CH Payment', '000001', '261019920001',
     0, '0', 'T', 'ACQQR', 'ONUS002', 10000.00, '360', '111117764535', '900002'),
    (97000000002026101902::numeric, 'ONUS_TEST_000002', '20261019', '120002',
     '9360002810000000000', 'Credit', '000002', '261019920002',
     0, '0', 'T', 'ACQQR', 'ONUS002', 20000.00, '360', '111117764535', '900002')
) sample(docid, sourceregnum, period, periodtime, pan, transactiontype,
         stan, rrn, reversalseq, rc, connection_acq, connection_iss,
         institutionbranch_acq, transactionamount, transactioncurrency,
         merchantid, auth_code)
WHERE NOT EXISTS (
    SELECT 1 FROM sw_replicate.on_doc_transaction existing
    WHERE existing.docid = sample.docid OR existing.rrn = sample.rrn
);

-- INI UPDATE YANG DIJALANKAN SETIAP MAU GENERATE ONUS LAGI.
-- SELECT DAG memfilter period = kemarin, bukan tanggal QR Recon/logical_date.
-- Pada 26 Oktober 2026, nilainya 20261025.
-- Gunakan tanggal kemarin menurut waktu proses Airflow existing.
-- Jika berbeda zona waktu dengan SQL ini, ganti ekspresi to_char dengan
-- literal tanggal yang tercetak/dihitung oleh proses Airflow, misal '20261025'.
UPDATE sw_replicate.on_doc_transaction
SET period = to_char(
    (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Jakarta')::date - 1,
    'YYYYMMDD'
)
WHERE (docid = 97000000002026101901 AND rrn = '261019920001'
       AND transactiontype = 'CH Payment')
   OR (docid = 97000000002026101902 AND rrn = '261019920002'
       AND transactiontype = 'Credit');

COMMIT;

-- CEK: harus keluar 2 baris sebelum trigger DAG.
-- Filter sama seperti SELECT ONUS di DAG, dibatasi pada RRN contoh ini.
SELECT merchantid AS merchant_id,
       institutionbranch_acq AS terminal_id,
       rrn AS retrieval_reference_number,
       period AS transaction_date, periodtime AS transaction_time,
       transactionamount AS transaction_amount,
       transactioncurrency AS transaction_amount_currency,
       pan AS customer_pan, rc AS response_code,
       CASE WHEN transactiontype = 'CH Payment' THEN 'D' ELSE 'C' END AS transaction_sign
FROM sw_replicate.on_doc_transaction odt
WHERE connection_acq = 'T'
  AND connection_iss IN ('ACQQR', 'a', 'O')
  AND pan LIKE '936000281%'
  AND transactiontype IN ('CH Payment', 'Credit')
  AND period = to_char((CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Jakarta')::date - 1, 'YYYYMMDD')
  AND (odt.rc::int = 0 OR (
      odt.rc::int IN (68, 82) AND EXISTS (
          SELECT 1 FROM sw_replicate.on_doc_transaction r
          WHERE r.rrn = odt.rrn AND r.transactiontype = 'Retail'
            AND r.rc::int = 0 AND r.reversalseq::int = 0
      )
  ))
  AND rrn IN ('261019920001', '261019920002')
ORDER BY transaction_time DESC;

-- =====================================================================
-- 2. DATABASE ENRICHMENT: JALANKAN SEKALI SEBELUM GENERATE PERTAMA
-- =====================================================================
BEGIN;

-- Bila merchant existing sudah ada, gunakan MID tersebut pada kedua INSERT
-- di bawah dan merchantid pada INSERT Way4. Merchant existing tidak di-update.
INSERT INTO merchant_tm (mid, merchant_criteria)
SELECT '111117764535', 'UMI'
WHERE NOT EXISTS (SELECT 1 FROM merchant_tm WHERE mid = '111117764535');

INSERT INTO qris_transaction_log_tt (
    rrn, mid, from_account_type, to_account_type,
    mpan, merchant_type, invoice_number, fee
)
SELECT r.rrn, '111117764535', '10', '20',
       '9360002801111177646', '5411', r.rrn, 0
FROM (VALUES ('261019920001'), ('261019920002')) r(rrn)
WHERE NOT EXISTS (SELECT 1 FROM qris_transaction_log_tt q WHERE q.rrn = r.rrn);

COMMIT;

-- CEK: harus keluar 2 baris dengan account type dan MPAN terisi.
SELECT q.rrn, q.mid, m.merchant_criteria, q.from_account_type,
       q.to_account_type, q.mpan, q.merchant_type, q.invoice_number, q.fee
FROM qris_transaction_log_tt q
JOIN merchant_tm m ON m.mid = q.mid
WHERE q.rrn IN ('261019920001', '261019920002');

-- =====================================================================
-- 3. ORACLE PWC: CEK MPAN DARI ENRICHMENT (JALANKAN PADA KONEKSI ORACLE)
-- =====================================================================
-- SELECT MER_ACCEPTOR_POINT_ID, MERCHANT_NUMBER
-- FROM MER_ACCEPTOR_POINT
-- WHERE TRIM(AP_ON_ID_10) = '9360002801111177646';
-- Harus ada hasil dengan MERCHANT_NUMBER numerik maksimal 15 digit.
-- Jika tidak ada, gunakan MPAN valid dari merchant existing di langkah 2.

-- SESUDAH DATA SIAP:
-- Trigger DAG dengan input QR Recon pada source FTP/SFTP yang dikonfigurasi.
-- Tanpa file input, process_file langsung return [] sehingga ONUS tidak di-query.
-- ONUS digabung ke POSTFLIN Rintis/PWC; target_path_onus belum dipakai flow utama.
-- Untuk uji hari berikutnya cukup ulangi UPDATE period + COMMIT pada langkah 1.
-- RRN tidak perlu diganti dan enrichment tidak perlu dibuat ulang.
