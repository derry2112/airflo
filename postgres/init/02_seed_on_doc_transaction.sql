INSERT INTO sw_replicate.on_doc_transaction (
    docid,
    sourceregnum,
    documenttype,
    period,
    periodtime,
    pan,
    transactiontype,
    stan,
    rrn,
    transactionstatus,
    reversalseq,
    settlementdate,
    rc,
    connection_acq,
    connection_iss,
    connection_dest,
    institution_acq,
    institution_iss,
    institution_dest,
    institutionbranch_acq,
    countrycode,
    countrycodenumeric,
    transactionamount,
    transactioncurrency,
    reconamount,
    reconcurrency,
    settlementamount,
    settlementcurrency,
    originalamount,
    originalcurrency,
    reversalamount,
    accountid_iss,
    accountid_dest,
    referencenumber,
    transactionchargeamount,
    transactionchargecurrency,
    settlementchargeamount,
    settlementchargecurrency,
    merchantid,
    auth_code,
    org_chrg_amt,
    addl_chrg_amt
)
SELECT *
FROM (
    VALUES
        (3706368150::NUMERIC, 'OD76015F8G4E', 'Y', '20240316', '211013', '6034399064330858', 'CHK_RCPT',       '091377', '407601091377', 'SUCCESS', 0, '20240317', '0',  'A', '0', NULL, '028',        '028', NULL,  '38000089',       'ID', 'IDN',       0::NUMERIC, '360',       0::NUMERIC, '360',       0::NUMERIC, '360',       0::NUMERIC, '360', 0::NUMERIC, '059610008332', NULL,           NULL,             0::NUMERIC, '360', 0::NUMERIC, '360', 'NISP38000089', '523514',    0::NUMERIC,    0::NUMERIC),
        (4604347320::NUMERIC, 'O2290425QL00', 'Y', '20240816', '061826', '6034398200388832', 'IB_TRF',         '226277', '000089951630', 'SUCCESS', 0, '20240817', '0',  '~', '~', 'A',  '777200028',  '028', '008', 'OM803746',       'ID', 'IDN', 4000000::NUMERIC, '360', 4000000::NUMERIC, '360', 4000000::NUMERIC, '360',       0::NUMERIC, '360', 0::NUMERIC, '010910102253', '1420013359475', '2024081606182037', 0::NUMERIC, '360', 0::NUMERIC, '360', 'EBWORXH2HMB',  NULL,     6500::NUMERIC,    0::NUMERIC),
        (   5372460::NUMERIC, 'O320050147O4', 'Y', '20241115', '101213', '5379408020146020', 'Retail',         NULL,     '090192210006', 'SUSPECT', 0, '20240516', '61', 'e', 'e', NULL, '009365',     '028', NULL,  'MTF TEST',       'ID', 'IDN',    2500::NUMERIC, '360',    2500::NUMERIC, '360',    2500::NUMERIC, '360',    2500::NUMERIC, '360', 0::NUMERIC, '080110205248', NULL,           NULL,             0::NUMERIC, '360', 0::NUMERIC, '360', 'ABC123TESTMTF19', NULL,        0::NUMERIC,    0::NUMERIC),
        (4604617430::NUMERIC, 'O22901CF3NOM', 'Y', '20240816', '073637', '6034399061975457', 'Balance Inquiry','634954', '000139614883', 'SUCCESS', 0, '20240817', '0',  '^', '0', NULL, '920',        '028', NULL,  '01004591',       'ID', 'IDN',      75::NUMERIC, '360',      75::NUMERIC, '360',      75::NUMERIC, '360',       0::NUMERIC, '360', 0::NUMERIC, '693815561909', NULL,           NULL,          4500::NUMERIC, '360', 4500::NUMERIC, '360', '01004591',       '729903', 4500::NUMERIC, 4500::NUMERIC),
        (4608357560::NUMERIC, 'O22901CG8MRL', 'Y', '20240816', '200741', '9360002810001894590','Retail',        '787582', '422920787582', 'SUCCESS', 0, '20240817', '0',  'q', '0', NULL, '9360999000', '028', NULL,  'A01',            'ID', 'IDN',  280000::NUMERIC, '360',  280000::NUMERIC, '360',  280000::NUMERIC, '360',       0::NUMERIC, '360', 0::NUMERIC, '565810102954', NULL,           NULL,             0::NUMERIC, '360', 0::NUMERIC, '360', 'ID107I000100084','641434',    0::NUMERIC,    0::NUMERIC),
        (4606460990::NUMERIC, 'O22901CFJIKL', 'Y', '20240816', '132513', '9360002810001418597','Retail',        '745317', '422920745317', 'SUCCESS', 0, '20240817', '0',  'q', '0', NULL, '9360999000', '028', NULL,  'A01',            'ID', 'IDN', 3900000::NUMERIC, '360', 3900000::NUMERIC, '360', 3900000::NUMERIC, '360',       0::NUMERIC, '360', 0::NUMERIC, NULL,           NULL,           NULL,             0::NUMERIC, '360', 0::NUMERIC, '360', '001910000718603','427600',    0::NUMERIC,    0::NUMERIC),
        (4609017470::NUMERIC, 'O22901CG9QVL', 'Y', '20240816', '203307', '6048209999999999993','IB_TRF',        '650613', '080032884137', 'SUCCESS', 0, '20240817', '0',  '^', '~', NULL, '535',        '535', '028', 'MB000045',      'ID', 'IDN',  975030::NUMERIC, '360',  975030::NUMERIC, '360',  975030::NUMERIC, '360',       0::NUMERIC, '360', 0::NUMERIC, NULL,           '265810030808',  NULL,             0::NUMERIC, '360', 0::NUMERIC, '360', 'MB0000450000000',NULL,        0::NUMERIC,    0::NUMERIC),
        (4608284010::NUMERIC, 'O22901CG3FDJ', 'Y', '20240816', '182507', '9360002810002819109','Retail',        '774509', '422920774509', 'SUCCESS', 0, '20240817', '0',  '^', '0', NULL, '9360999000', '028', NULL,  '72326321',       'ID', 'IDN',       1::NUMERIC, '360',       1::NUMERIC, '360',       1::NUMERIC, '360',       1::NUMERIC, '360', 1::NUMERIC, '040110001473', NULL,           NULL,             0::NUMERIC, '360', 0::NUMERIC, '360', '723263218',      '245145',    0::NUMERIC,    0::NUMERIC),
        (   4084210::NUMERIC, 'O057011QG4T1', 'Y', '20240226', '101900', '9360002810100000461','Retail',        '600545', '405720600545', 'SUSPECT', 1, '20240227', '0',  'q', '0', NULL, '9360999000', '028', NULL,  'OPENwQs',        'ID', 'IDN',       0::NUMERIC, '360',       0::NUMERIC, '360',       0::NUMERIC, '360',       0::NUMERIC, '360', 0::NUMERIC, NULL,           NULL,           NULL,             0::NUMERIC, '360', 0::NUMERIC, '360', 'Vz2dVqCfZlzsxyK','737140',    0::NUMERIC,    0::NUMERIC),
        (3703595760::NUMERIC, 'OD76015EJ6BB', 'Y', '20240316', '090710', '6034399065402789', 'CHK_RCPT',       '027373', '407601027373', 'SUCCESS', 0, '20240316', '0',  'A', '0', NULL, '028',        '028', NULL,  '28000534',       'ID', 'IDN',   24000::NUMERIC, '360',   24000::NUMERIC, '360',   24000::NUMERIC, '360',       0::NUMERIC, '360', 0::NUMERIC, NULL,           NULL,           NULL,             0::NUMERIC, '360', 0::NUMERIC, '360', 'NISP28000534',  '820106',    0::NUMERIC,    0::NUMERIC),
        (4608736800::NUMERIC, 'O22901CG7J67', 'Y', '20240816', '194440', '9360991507288629341','CH Payment',    '784694', '422920784691', 'SUCCESS', 0, '20240816', '0',  'A', '0', NULL, '9360999000', '028', NULL,  '72886293',       'ID', 'IDN',     986::NUMERIC, '392',    1308::NUMERIC, '392',  107774::NUMERIC, '392',       0::NUMERIC, '392', 0::NUMERIC, NULL,           NULL,           NULL,             0::NUMERIC, '392', 0::NUMERIC, '392', '728862934',     '628206',    0::NUMERIC,    0::NUMERIC)
) AS seed (
    docid, sourceregnum, documenttype, period, periodtime, pan,
    transactiontype, stan, rrn, transactionstatus, reversalseq,
    settlementdate, rc, connection_acq, connection_iss, connection_dest,
    institution_acq, institution_iss, institution_dest, institutionbranch_acq,
    countrycode, countrycodenumeric, transactionamount, transactioncurrency,
    reconamount, reconcurrency, settlementamount, settlementcurrency,
    originalamount, originalcurrency, reversalamount, accountid_iss,
    accountid_dest, referencenumber, transactionchargeamount,
    transactionchargecurrency, settlementchargeamount,
    settlementchargecurrency, merchantid, auth_code, org_chrg_amt, addl_chrg_amt
)
WHERE NOT EXISTS (
    SELECT 1
    FROM sw_replicate.on_doc_transaction existing
    WHERE existing.docid = seed.docid
);

-- Nilai tanggal lama dari foto tetap dipertahankan pada blok INSERT di atas.
-- Keterangan: untuk pengujian DAG lokal, period dan settlementdate disamakan
-- dengan tanggal saat seed dijalankan agar query get_way4_data menemukan data.
UPDATE sw_replicate.on_doc_transaction
SET period = TO_CHAR(CURRENT_DATE, 'YYYYMMDD'),
    settlementdate = TO_CHAR(CURRENT_DATE, 'YYYYMMDD')
WHERE docid IN (
    3706368150,
    4604347320,
    5372460,
    4604617430,
    4608357560,
    4606460990,
    4609017470,
    4608284010,
    4084210,
    3703595760,
    4608736800
);
