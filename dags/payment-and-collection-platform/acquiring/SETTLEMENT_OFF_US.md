# Settlement off-us D4

`dag_settlement_off_us.py` contains the DAG, D4 parser, and database persistence
in one file. It uses Airflow 3 TaskFlow to download SFTP ZIPs matching
`config['file_zip']`, archive them in S3, and extract only the configured D4 mask
(case insensitive), with a separate today-prefix filter (YYMMDD, WIB) for TXT
names as well as ZIP names. Archive entries are streamed to local basenames rather than
extracting paths from the ZIP. Each D4 file is parsed in full before any database
writes. XCom carries file locations and load counts, not transaction/card data.

Configure the Airflow connections `SFTP_EBSP`, `s3-artefact`, and `db_rekon_uat`.
The source directory remains `/mtiuat/toocbc/QRRecon/ClearingRintisQRIS/`.
The existing manual schedule is retained. The ZIP mask is `*NPG_RPT.zip`. A separate filter requires the filename
to start with today at task execution in the DAG timezone (UTC+7/WIB),
formatted YYMMDD. It does not use the logical date of a historical run.
For example, 31 October 2026 matches `261031NPG_RPT.zip`; the original
`311030NPG_RPT.zip` test archive does not match this date filter.

Apply the existing `dags/sql/rekon.sql` schema to the target PostgreSQL database
and apply `dags/sql/settlement_ingestion_controls.sql` before the first run.
The DAG does not create tables at runtime. It writes:

- `sw_rekon.acqpwc_file_log_tt`: one row per successfully loaded D4 filename;
  filepath is the source SFTP directory, filedate is the filename's first six
  digits, issuername comes from the report ISSUER header, transactiontype is D4.
- `sw_rekon.acqpwc_d4summary_tt`: one row per ACQUIRER bank and report period
  within the file. Repeated page headers are merged. Zero-transaction sections
  are retained. Frequencies and amounts are calculated from detail records.
  `issuer` retains the complete ACQUIRER label; `institutionid` uses an explicit
  numeric code such as 360001, and stays NULL for alphabetic-only codes such as
  BC. No bank-code mapping is inferred.
- `sw_rekon.acqpwc_d4detail_tt`: one row per complete two-line transaction,
  linked to its bank summary. Monetary values use Decimal. Leading zeros in
  references, authorization codes, trace numbers and terminal IDs are retained.

All three writes commit together per D4 file. Errors roll back the entire file.
An advisory transaction lock and a log lookup prevent repeat inserts for the
same D4 filename; a corrected file with the same name requires a deliberate
reprocessing procedure. Other applications writing these tables must follow
the same locking convention for this duplicate protection to apply.

The transcribed sample TXT and its ZIP include the continuation of BCA
transaction 64 from the additional reference photo. The full sample passes
parser validation with 1,073 complete transactions. Incomplete transactions
are rejected rather than silently dropped.

Run the parser and transaction-control tests with:

```sh
python3 -m unittest discover -s tests -p 'test_settlement_d4.py' -v
```

These tests do not connect to SFTP, S3, or PostgreSQL. Validate the complete DAG
in the configured Airflow environment before running it against those services.

Transfer integrity checks compare source size/mtime before and after SFTP download
(FTP fallback checks size), validate every ZIP entry's CRC, and check extracted
TXT size against its ZIP entry. SHA-256 and byte counts are passed through XCom.
Every S3 upload is read back completely and verified; subsequent ZIP/TXT downloads
must match the expected SHA-256 and size before extraction or parsing. Read errors,
invalid UTF-8, NUL bytes, empty D4 members and incomplete transaction records fail
the task before database writes for that D4 file. This adds full reads of S3 objects
and temporary disk space for the TXT to the existing processing cost.

These checks detect transfer corruption relative to the downloaded source. They
cannot prove that a valid ZIP was produced from a complete original report. If the
sender removes complete transactions before creating the ZIP, detecting that loss
requires a trusted sender checksum, expected record count, or validated report
trailer. SALES footer totals are validated when present; sections without
complete footers are explicitly recorded as completeness-unconfirmed in audit.

Transfer recovery and quarantine:

- `transfer_attempts=3` limits each SFTP/FTP download and S3 upload/download to
  three attempts for transient failures, with exponential backoff based on
  `transfer_retry_delay_seconds=2` (2 seconds, then 4 seconds by default).
  Validation, permission and missing-file errors are not retried. Source
  connections are reopened for each attempt. Database writes are not retried by
  this mechanism.
- `txt_encoding='utf-8-sig'` is explicit and strict. Change it only to an encoding
  confirmed by the sender; the DAG never ignores decoding errors.
- A rejected file is recorded under
  `artefacts/settlement_off-us/quarantine/<stage>/<unique-id>/<date>/<run-id>/`
  in the configured S3 bucket. The local bytes, when available, and `reason.json`
  are uploaded and verified. If no local bytes are available, the manifest retains
  the original S3/SFTP location. `reason.json` includes filename, stage, error type,
  reason (including the parser line number when applicable), and run ID.
- Rejected records pass through subsequent tasks without being loaded. Valid
  independent ZIPs and D4 members continue. ZIP-level CRC failure rejects that
  archive before extraction. The final `show_result` task fails visibly when any
  file was rejected. Search task logs for `FILE RESULT` or `FILE REJECTED` and open
  the indicated quarantine manifest. A quarantine-storage failure is explicitly
  marked `quarantine_failed`; it is never reported as successfully quarantined.
- Correct the source file and rerun the DAG. Failed files have no committed file
  log and can be processed; successfully loaded filenames remain protected from
  duplicates. Do not delete successful database rows merely to retry failures.

Current operating contract: ZIP only (`require_sender_manifest=False`).
Readiness uses two source size/mtime snapshots 5 seconds apart and a minimum
SFTP file age of 30 seconds. Permanent audit, unique active-file constraints,
versioned corrections, migration requirements, and operator procedures are
described in [the operational notes](../../../../docs/settlement_ingestion_operasional.md).
