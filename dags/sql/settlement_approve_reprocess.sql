-- Run only by an authorized operator after reviewing both file versions.
-- psql -v ON_ERROR_STOP=1 -v filename=251030D4_NPG.txt \
--   -v previous_sha=<verified_current_sha256> -v expected_sha=<reviewed_new_sha256> \
--   -v reason='Approved correction ticket XYZ' -f settlement_approve_reprocess.sql
\set ON_ERROR_STOP on
BEGIN;
SELECT pg_advisory_xact_lock(hashtext(lower(:'filename')));
SELECT fileid AS current_fileid
FROM sw_rekon.acqpwc_file_log_tt
WHERE lower(filename)=lower(:'filename') AND transactiontype='D4'
  AND isactive AND contentsha256=:'previous_sha'
\gset
-- \gset requires exactly one matching current version; otherwise psql stops.
INSERT INTO sw_rekon.acqpwc_reprocess_request_tt
    (filename, previoussha256, expectedsha256, approvedby, reason, oldfileid)
VALUES (:'filename', :'previous_sha', :'expected_sha', current_user, :'reason', :current_fileid)
RETURNING requestid, filename, status, approvedby, approvedat;
COMMIT;
