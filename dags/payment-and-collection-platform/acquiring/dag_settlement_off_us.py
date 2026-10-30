from datetime import datetime, timezone, timedelta
import logging
import hashlib
import json
import time
import uuid
import re
from decimal import Decimal
import os, fnmatch, shutil, zipfile, sys
from pathlib import Path

from utils.s3_artefact import S3ArtefactHook
from utils.hooks.S3HookOcbc import S3HookOcbc
from tempfile import TemporaryDirectory
from urllib.parse import urlsplit
from typing import List
from airflow.sdk import dag, task, get_current_context

from utils.ftp import FTPHookOcbc
from utils.sftp import SFTPHookOcbc
from utils.postgres import PostgresEtlHook
from utils.callback import Callback
from utils.telemetry.tracing import tracer
from dags.utils.models.airflow_constant import Owner

_project_root = str(Path(__file__).resolve().parents[3])
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

def is_transient_transfer_error(error):
    """Retry transport/service availability failures, not invalid business input."""
    if isinstance(error, (FileNotFoundError, PermissionError, ValueError, zipfile.BadZipFile)):
        return False
    response = getattr(error, 'response', None)
    if isinstance(response, dict):
        status = response.get('ResponseMetadata', {}).get('HTTPStatusCode', 0)
        code = response.get('Error', {}).get('Code', '')
        return status in (408, 429) or status >= 500 or code in (
            'SlowDown', 'RequestTimeout', 'Throttling', 'ServiceUnavailable')
    if isinstance(error, OSError):
        return True
    return type(error).__name__ in (
        'EndpointConnectionError', 'ConnectionClosedError', 'ReadTimeoutError',
        'ConnectTimeoutError', 'SSHException', 'NoValidConnectionsError')


def source_is_stable(before, after, now, minimum_age_seconds):
    return (before == after and before[0] > 0 and
            (after[1] is None or now - after[1] >= minimum_age_seconds))


def select_ready_archives(names, expected, require_sidecars=False):
    available = set(names)
    candidates = sorted(name for name in available
                        if fnmatch.fnmatchcase(name.upper(), expected.upper()))
    ready, waiting = [], []
    for name in candidates:
        (ready if not require_sidecars or (name + '.done' in available and name + '.manifest.json' in available)
         else waiting).append(name)
    return ready, waiting


def validate_sender_manifest(manifest, archive_name, file_mask='*D4_NPG.txt'):
    """Versioned sender contract; all totals are decimal strings, never floats."""
    def require(condition, message):
        if not condition:
            raise ValueError('Manifest: ' + message)

    def fingerprint(entry):
        require(type(entry.get('size')) is int and entry['size'] > 0, 'invalid size')
        require(isinstance(entry.get('sha256'), str) and
                re.fullmatch(r'[0-9a-f]{64}', entry['sha256']), 'invalid SHA-256')

    def total(entry):
        require(type(entry.get('transaction_count')) is int and
                entry['transaction_count'] >= 0, 'invalid transaction count')
        require(isinstance(entry.get('total_amount'), str) and
                re.fullmatch(r'\d{1,17}\.\d{2}', entry['total_amount']), 'invalid total amount')

    require(isinstance(manifest, dict) and type(manifest.get('schema_version')) is int
            and manifest['schema_version'] == 1,
            'unsupported schema version')
    archive = manifest.get('archive')
    require(isinstance(archive, dict) and archive.get('filename') == archive_name,
            'archive filename mismatch')
    fingerprint(archive)
    files = manifest.get('files')
    require(isinstance(files, list) and len(files) > 0, 'missing D4 files')
    names = set()
    for entry in files:
        require(isinstance(entry, dict), 'invalid file entry')
        name = entry.get('filename')
        require(isinstance(name, str) and name not in ('', '.', '..') and
                '/' not in name and '\\' not in name and
                fnmatch.fnmatchcase(name.upper(), file_mask.upper()), 'invalid D4 filename')
        require(name.upper() not in names, 'duplicate D4 filename')
        names.add(name.upper())
        fingerprint(entry)
        total(entry)
        banks = entry.get('banks')
        require(isinstance(banks, list) and len(banks) > 0, 'missing bank totals')
        keys = set()
        for bank in banks:
            require(isinstance(bank, dict) and isinstance(bank.get('acquirer'), str)
                    and bank['acquirer'].strip(), 'invalid bank')
            require(isinstance(bank.get('period'), str), 'invalid period')
            datetime.strptime(bank['period'], '%Y-%m-%d')
            key = (bank['acquirer'], bank['period'])
            require(key not in keys, 'duplicate bank/period')
            keys.add(key)
            total(bank)
        require(sum(bank['transaction_count'] for bank in banks) == entry['transaction_count'],
                'bank counts do not match file count')
        require(sum((Decimal(bank['total_amount']) for bank in banks), Decimal('0')) ==
                Decimal(entry['total_amount']), 'bank amounts do not match file amount')
    return manifest


def reconcile_report(report, expected):
    actual = {(group['issuer'], group['period'].isoformat()):
              (group['frekuensi'], group['totalamount']) for group in report['groups']}
    declared = {(bank['acquirer'], bank['period']):
                (bank['transaction_count'], Decimal(bank['total_amount']))
                for bank in expected['banks']}
    if actual != declared:
        raise ValueError('Reconciliation failed: bank/period counts or amounts differ from sender manifest')
    if (sum(group['frekuensi'] for group in report['groups']) != expected['transaction_count']
            or sum((group['totalamount'] for group in report['groups']), Decimal('0'))
            != Decimal(expected['total_amount'])):
        raise ValueError('Reconciliation failed: file count or amount differs from sender manifest')


def write_ingestion_audit(cursor, item, run_id, status, stage, details=None, fileid=None):
    integrity = item.get('d4_integrity') or item.get('zip_integrity') or {}
    cursor.execute(
        'INSERT INTO sw_rekon.acqpwc_ingestion_audit_tt '
        '(runid, filename, zipfilename, contentsha256, status, stage, details, fileid) '
        'VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s)',
        (run_id, item['filename'], item.get('zipfilename'), integrity.get('sha256'),
         status, stage, json.dumps(details or {}), fileid))


def retry_transfer(operation, label, attempts=3, delay_seconds=2):
    if attempts < 1 or delay_seconds < 0:
        raise ValueError('Invalid transfer retry configuration')
    for attempt in range(1, attempts + 1):
        try:
            return operation()
        except Exception as error:
            logging.warning('TRANSFER FAILED: %s attempt=%s/%s error_type=%s',
                            label, attempt, attempts, type(error).__name__)
            if attempt == attempts or not is_transient_transfer_error(error):
                raise
            time.sleep(min(delay_seconds * (2 ** (attempt - 1)), 30))


def process_isolated(items, operation, on_failure):
    results = []
    for item in items:
        if item.get('status') in ('quarantined', 'quarantine_failed'):
            results.append(item)
            continue
        try:
            results.extend(operation(item))
        except Exception as error:
            results.append(on_failure(item, error))
    return results


def fingerprint_stream(stream):
    digest = hashlib.sha256()
    size = 0
    for chunk in iter(lambda: stream.read(1024 * 1024), b''):
        size += len(chunk)
        digest.update(chunk)
    return {'size': size, 'sha256': digest.hexdigest()}


def fingerprint_file(path):
    with open(path, 'rb') as stream:
        return fingerprint_stream(stream)


def verify_fingerprint(actual, expected, label):
    if actual != expected:
        raise ValueError(f'Integrity check failed for {label}: size/SHA-256 mismatch')


def validate_zip(path):
    if not zipfile.is_zipfile(path):
        raise ValueError(f'Invalid or truncated ZIP: {os.path.basename(path)}')
    with zipfile.ZipFile(path) as archive:
        bad_member = archive.testzip()
        if bad_member:
            raise ValueError(f'ZIP CRC check failed: {bad_member}')


def extract_verified_member(archive, member, destination):
    if member.is_dir() or member.file_size == 0:
        raise ValueError(f'Empty D4 member: {member.filename}')
    try:
        with archive.open(member) as source, open(destination, 'wb') as target:
            shutil.copyfileobj(source, target)
        actual = fingerprint_file(destination)
        if actual['size'] != member.file_size:
            raise ValueError(f'Extracted size mismatch: {member.filename}')
        return actual
    except Exception:
        Path(destination).unlink(missing_ok=True)
        raise


DETAIL_FIELDS = (
    'transactiondate', 'transactiontime', 'transactioncode', 'transactiontype',
    'cardno', 'transactionamount', 'interchangefee', 'authcode', 'traceno',
    'refno', 'termid', 'merchantid', 'merchantname',
)
_FIRST = re.compile(
    r'^\s*\d+\s+(\d{2}/\d{2}/\d{2})\s+(\d{2}:\d{2}:\d{2})\s+'
    r'(\d+)\s+(\d+)\s+(\d+)\s+([\d,]+\.\d{2})\s+'
    r'([\d,]+\.\d{2})\s+(\S+)\s+(\S+)\s*$'
)
_SECOND = re.compile(r'^\s*(\d+)\s+(\S+)\s+(\d+-\d+)\s+(.+?)\s*$')


def parse_d4(lines):
    groups = {}
    footer_positions = {}
    footer_checks = {}
    bank = period = pending = issuer = None
    current = None
    for lineno, line in enumerate(lines, 1):
        if '\x00' in line:
            raise ValueError(f'D4 line {lineno}: unexpected NUL byte')
        line = line.rstrip('\r\n\f')
        if pending is not None:
            match = _SECOND.match(line)
            if not match:
                raise ValueError(f'D4 line {lineno}: missing continuation for transaction at line {pending[0]}')
            pending[1].update(zip(DETAIL_FIELDS[9:], match.groups()))
            current['details'].append(pending[1])
            pending = None
            continue
        match = re.search(r'ACQUIRER:\s*(.+?)(?:\s{2,}TANGGAL\s*:|$)', line)
        if match:
            bank = match.group(1).strip()
            period = None
            current = None
        match = re.search(r'TANGGAL\s*:\s*(\d{2}-\d{2}-\d{2})', line)
        if match:
            period = datetime.strptime(match.group(1), '%d-%m-%y').date()
        match = re.search(r'ISSUER\s*:\s*(.+?)\s*$', line)
        if match:
            issuer = match.group(1).strip()
        if bank and period:
            key = (bank, period)
            numeric_code = re.match(r'^[A-Z]+-(\d+)\b', bank)
            current = groups.setdefault(key, {
                'issuer': bank, 'institutionid': int(numeric_code.group(1)) if numeric_code else None,
                'period': period, 'details': [],
            })
        if re.match(r'^0\s+SALES\b', line):
            footer = re.fullmatch(r'0\s+SALES\s+(\d+)\s+([\d,]+\.\d{2})\s*', line)
            if current is None or footer is None:
                raise ValueError(f'D4 line {lineno}: invalid SALES footer')
            key = (bank, period)
            section = current['details'][footer_positions.get(key, 0):]
            if (len(section) != int(footer.group(1)) or
                    sum((row['transactionamount'] for row in section), Decimal('0'))
                    != Decimal(footer.group(2).replace(',', ''))):
                raise ValueError(f'D4 line {lineno}: SALES footer count/amount mismatch')
            footer_positions[key] = len(current['details'])
            footer_checks[key] = footer_checks.get(key, 0) + 1
            continue
        match = _FIRST.match(line)
        if match:
            if current is None:
                raise ValueError(f'D4 line {lineno}: transaction without bank/period')
            date, time, code, kind, card, amount, fee, auth, trace = match.groups()
            detail = dict(zip(DETAIL_FIELDS[:9], (
                datetime.strptime(date, '%d/%m/%y').date(),
                datetime.strptime(time, '%H:%M:%S').time(), code, kind, card,
                Decimal(amount.replace(',', '')), Decimal(fee.replace(',', '')), auth, trace,
            )))
            pending = (lineno, detail)
        elif re.match(r'^\s*\d+\s+\d{2}/\d{2}/\d{2}\b', line):
            raise ValueError(f'D4 line {lineno}: malformed transaction')
    if pending is not None:
        raise ValueError(f'D4 line {pending[0]}: incomplete transaction at end of file')
    if not groups:
        raise ValueError('D4 report contains no bank/period sections')
    for group in groups.values():
        group['frekuensi'] = len(group['details'])
        group['totalamount'] = sum((row['transactionamount'] for row in group['details']), Decimal('0.00'))
    missing = [{'acquirer': key[0], 'period': key[1].isoformat()}
               for key, group in groups.items()
               if key not in footer_checks or footer_positions[key] != len(group['details'])]
    return {'issuername': issuer, 'groups': list(groups.values()),
            'controls': {'validated_footers': sum(footer_checks.values()),
                         'banks_without_complete_footer': missing}}



def persist_d4(connection, item, report, run_id):
    try:
        if item.get('expected') is not None:
            reconcile_report(report, item['expected'])
        checksum = item['d4_integrity']['sha256']
        if not re.fullmatch(r'[0-9a-f]{64}', checksum):
            raise ValueError('Missing verified D4 checksum')
        with connection.cursor() as cursor:
            cursor.execute('SELECT pg_advisory_xact_lock(hashtext(%s))', (item['filename'].lower(),))
            cursor.execute(
                'SELECT fileid, contentsha256, fileversion FROM sw_rekon.acqpwc_file_log_tt '
                "WHERE lower(filename) = lower(%s) AND transactiontype = 'D4' AND isactive FOR UPDATE",
                (item['filename'],))
            existing = cursor.fetchone()
            requestid = previous_id = None
            version = 1
            if existing:
                previous_id, previous_checksum, version = existing
                if previous_checksum == checksum:
                    write_ingestion_audit(cursor, item, run_id, 'ALREADY_PROCESSED',
                                          'database', fileid=previous_id)
                    connection.commit()
                    return {'filename': item['filename'], 'fileid': previous_id,
                            'status': 'already_processed', 'version': version}
                if previous_checksum is None:
                    raise ValueError('Existing file has no verified checksum; operator review required')
                cursor.execute(
                    'SELECT requestid FROM sw_rekon.acqpwc_reprocess_request_tt '
                    "WHERE lower(filename)=lower(%s) AND previoussha256=%s AND expectedsha256=%s "
                    "AND status='APPROVED' FOR UPDATE",
                    (item['filename'], previous_checksum, checksum))
                approval = cursor.fetchone()
                if not approval:
                    raise ValueError('Changed content for existing filename requires an approved reprocess request')
                requestid = approval[0]
                cursor.execute('UPDATE sw_rekon.acqpwc_file_log_tt SET isactive=FALSE WHERE fileid=%s',
                               (previous_id,))
                version += 1
            filedate = re.match(r'(\d{6})', item['filename'])
            if not filedate:
                raise ValueError('D4 filename must start with a six-digit file date')
            cursor.execute(
                'INSERT INTO sw_rekon.acqpwc_file_log_tt '
                '(filepath, filename, transactiontype, insetby, issuername, filedate, '
                'contentsha256, fileversion, replacesfileid) '
                'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING fileid',
                (item.get('source_path', ''), item['filename'], 'D4', 'dag_settlement_off-us',
                 report['issuername'], filedate.group(1), checksum, version, previous_id))
            fileid = cursor.fetchone()[0]
            for group in report['groups']:
                cursor.execute(
                    'INSERT INTO sw_rekon.acqpwc_d4summary_tt '
                    '(fileid, institutionid, issuer, period, frekuensi, totalamount) '
                    'VALUES (%s, %s, %s, %s, %s, %s) RETURNING summaryid',
                    (fileid, group['institutionid'], group['issuer'], group['period'],
                     group['frekuensi'], group['totalamount']))
                summaryid = cursor.fetchone()[0]
                if group['details']:
                    cursor.executemany(
                        'INSERT INTO sw_rekon.acqpwc_d4detail_tt (summaryid, '
                        + ', '.join(DETAIL_FIELDS) + ') VALUES ('
                        + ', '.join(['%s'] * (len(DETAIL_FIELDS) + 1)) + ')',
                        [(summaryid, *(row[field] for field in DETAIL_FIELDS)) for row in group['details']])
            if requestid is not None:
                cursor.execute(
                    "UPDATE sw_rekon.acqpwc_reprocess_request_tt SET status='APPLIED', "
                    'appliedat=CURRENT_TIMESTAMP, oldfileid=%s, newfileid=%s WHERE requestid=%s',
                    (previous_id, fileid, requestid))
            write_ingestion_audit(cursor, item, run_id, 'LOADED', 'database',
                                  {'version': version, 'reprocess_request_id': requestid,
                                   'report_controls': report.get('controls')}, fileid)
        connection.commit()
        return {'filename': item['filename'], 'fileid': fileid, 'status': 'inserted',
                'version': version, 'summaries': len(report['groups']),
                'details': sum(group['frekuensi'] for group in report['groups'])}
    except Exception:
        connection.rollback()
        raise


class GenericSFTPHook:
    def __init__(self, ssh_conn_id):
        self.hook = SFTPHookOcbc(ssh_conn_id=ssh_conn_id)
        self.conn = None

    def get_conn(self):
        if self.conn is None:
            self.conn = self.hook.get_conn()
        return self.conn

    def listdir(self, path):
        if self.conn is None:
            self.conn = self.hook.get_conn()

        return self.conn.listdir(path)

    def retrieve_file(self, remote_path, local_path):
        if self.conn is None:
            self.conn = self.hook.get_conn()
        return self.conn.get(remote_path, local_path)

    def store_file(self, remote_path, local_path):
        if self.conn is None:
            self.conn = self.hook.get_conn()
        return self.conn.put(local_path, remote_path)

    def close_conn(self):
        if self.conn:
            self.conn.close()
            self.conn = None

db_rekon = PostgresEtlHook()

config = {
    'workflow_name': 'dag_settlement_off-us',
    "owner": Owner.PAYMENT_AND_COLLECTION_PLATFORM,
    'tags': ['data_payment', 'acquiring', 'splitfile', 'data_payment', 'core'],
    'schedule_interval': '0 5-23/2 * * *',
    'start_date': '2023-01-01',
    'email_list': [],

    'transfer_attempts': 3,
    'transfer_retry_delay_seconds': 2,
    'txt_encoding': 'utf-8-sig',
    'require_sender_manifest': False,
    'source_stability_seconds': 5,
    'source_min_age_seconds': 30,
    'manifest_max_bytes': 1024 * 1024,
    #DB
    'db_rekon': 'db_rekon_uat',

    'source_connection': 'SFTP_EBSP',
    'source_path': '/mtiuat/toocbc/QRRecon/ClearingRintisQRIS/',
    'source_connection_type': 'SFTP',

    'file_zip': '{date}NPG_RPT.zip',
    'file_txt': '*D4_NPG.txt',

    'input_s3_app': 'settlement_off-us',
    'result_s3_app_rintis': 'settlement_off-us',
}

FILE_PATH = os.path.dirname(os.path.realpath(__file__))
workflow_name = config["workflow_name"]
dag_name = workflow_name
callback = Callback(email_list=config["email_list"])

user_cron = None
start_date = config["start_date"]
tz = timezone(timedelta(hours=7))
app_name = os.path.basename(os.path.dirname(FILE_PATH))
job_type = "file"
job_operation = "posting"
dynamic_tags = list(dict.fromkeys([
    *config["tags"],
    f"app:{app_name}",
    f"job:{job_type}",
    f"job:{job_type}:{job_operation}",
    f"owner:{config['owner']}",
]))
default_args = {
    "owner": config["owner"],
    "on_failure_callback": callback.task_failure_alert,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "priority_weight": 10,
    "execution_timeout": timedelta(minutes=10),
}


@dag(
    dag_id=workflow_name,
    schedule=user_cron,
    description="Ambil file zip, unzip file rintis, dan insert hasilnya ke db rekon",
    default_args=default_args,
    start_date=datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=tz),
    catchup=False,
    dagrun_timeout=timedelta(minutes=10),
    tags=dynamic_tags,
    max_active_runs=1,
    on_success_callback=callback.dag_success_alert,
)
def dag_settlement_off_us():

    @task
    @tracer.start_as_current_span(f"{dag_name}_task_get_unprocessed_file")
    def get_unprocessed_files() -> List[str]:

        source = _hook(config['source_connection_type'],config['source_connection'])
        try:
            files = sorted(source.listdir(config['source_path']))
            logging.info("SOURCE PATH: %s",config['source_path'] )
            logging.info("FILES FOUND: %s",files)

            file_date = datetime.now(tz).strftime("%y%m%d")
            expected_file = config['file_zip'].replace('{date}', file_date)
            logging.info("EXPECTED FILE : %s", expected_file)
            candidates, waiting = select_ready_archives(
                files, expected_file, config['require_sender_manifest'])
            snapshots = {}
            remote_conn = source.get_conn()
            for name in candidates:
                path = f"{config['source_path'].rstrip('/')}/{name}"
                attrs = remote_conn.stat(path) if config['source_connection_type'] == 'SFTP' else None
                snapshots[name] = ((attrs.st_size, attrs.st_mtime) if attrs is not None
                                   else (remote_conn.size(path), None))
            if snapshots:
                time.sleep(config['source_stability_seconds'])
            ready = []
            for name, before in snapshots.items():
                path = f"{config['source_path'].rstrip('/')}/{name}"
                attrs = remote_conn.stat(path) if config['source_connection_type'] == 'SFTP' else None
                after = ((attrs.st_size, attrs.st_mtime) if attrs is not None
                         else (remote_conn.size(path), None))
                if source_is_stable(before, after, time.time(), config['source_min_age_seconds']):
                    ready.append(name)
                else:
                    waiting.append(name)
            for name in waiting:
                logging.warning('WAITING READY: %s source changing, too recent, or sidecars required', name)
                _audit({'filename': name}, 'WAITING_READY', 'discovery')
            logging.info('READY FILES: %s', ready)
            return ready
        finally:
            source.close_conn()

    def _hook(connection_type, connection_id):
        if connection_type == "FTP":
            return FTPHookOcbc(ftp_conn_id=connection_id)

        if connection_type == "SFTP":
            return GenericSFTPHook(ssh_conn_id=connection_id)

        raise ValueError(
            f"Unsupported connection type: {connection_type}"
        )

    def _audit(item, status, stage, details=None):
        connection = db_rekon.__setconnection_cursor__(config['db_rekon'])
        try:
            with connection.cursor() as cursor:
                write_ingestion_audit(cursor, item, get_current_context()['dag_run'].run_id,
                                      status, stage, details)
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _retry(operation, label):
        return retry_transfer(operation, label, config['transfer_attempts'],
                              config['transfer_retry_delay_seconds'])

    def _upload_result_to_s3(filepath, app_name, date):
        return _retry(lambda: _upload_once(filepath, app_name, date),
                      f'S3 upload {os.path.basename(filepath)}')

    def _upload_once(filepath, app_name, date):
        logging.info("UPLOAD APP_NAME=%s", app_name)
        expected = fingerprint_file(filepath)
        run_id = get_current_context()['dag_run'].run_id
        s3_url = S3ArtefactHook().put_file_to_s3(
            workflow_name=f'{run_id}/{os.path.basename(filepath)}',
            app_name=app_name,
            file_path=filepath,
            ext=None,
            date_file=date.strftime('%Y%m%d'),
            is_delete_immediately=False,
        )
        if not isinstance(s3_url, str) or not s3_url:
            raise ConnectionError('S3ArtefactHook upload failed; original cause is in utility logs')
        parsed = urlsplit(s3_url)
        if parsed.scheme != 's3' or not parsed.netloc or not parsed.path.lstrip('/'):
            raise ValueError('S3ArtefactHook returned an invalid S3 URL')
        client = S3HookOcbc(aws_conn_id='s3-artefact').get_conn()
        response = client.get_object(Bucket=parsed.netloc, Key=parsed.path.lstrip('/'))
        body = response['Body']
        try:
            actual = fingerprint_stream(body)
        finally:
            body.close()
        verify_fingerprint(actual, expected, s3_url)
        logging.info('S3 VERIFIED: %s size=%s sha256=%s', s3_url, expected['size'], expected['sha256'])
        return s3_url

    def _download_result_from_s3(s3_url, directory, expected):
        return _retry(lambda: _download_once(s3_url, directory, expected),
                      f'S3 download {os.path.basename(urlsplit(s3_url).path)}')

    def _download_once(s3_url, directory, expected):
        parsed = urlsplit(s3_url)
        key = parsed.path.lstrip('/')
        if parsed.scheme != 's3' or not parsed.netloc or not key:
            raise ValueError(f'URL hasil S3 tidak valid: {s3_url}')
        filename = os.path.basename(key)
        if filename in ('', '.', '..'):
            raise ValueError(f'Nama file hasil S3 tidak valid: {s3_url}')
        local_file = os.path.join(directory, filename)
        client = S3HookOcbc(aws_conn_id='s3-artefact').get_conn()
        client.download_file(parsed.netloc, key, local_file)
        verify_fingerprint(fingerprint_file(local_file), expected, s3_url)
        return local_file

    def _quarantine(item, stage, error, local_file=None):
        record = {**item, 'status': 'quarantined', 'stage': stage,
                  'error_type': type(error).__name__, 'reason': str(error),
                  'run_id': get_current_context()['dag_run'].run_id,
                  'quarantined_at': datetime.now(timezone.utc).isoformat()}
        app = f"{config['result_s3_app_rintis']}/quarantine/{stage}/{uuid.uuid4().hex}"
        try:
            if local_file and os.path.isfile(local_file):
                record['quarantine_file_s3_url'] = _upload_result_to_s3(
                    local_file, app, datetime.now())
            with TemporaryDirectory() as folder:
                manifest = os.path.join(folder, 'reason.json')
                with open(manifest, 'w', encoding='utf-8') as stream:
                    json.dump(record, stream, indent=2)
                record['quarantine_manifest_s3_url'] = _upload_result_to_s3(
                    manifest, app, datetime.now())
        except Exception as quarantine_error:
            record.update(status='quarantine_failed',
                          quarantine_error_type=type(quarantine_error).__name__)
        try:
            _audit(record, record['status'].upper(), stage,
                   {key: record[key] for key in ('error_type', 'reason',
                    'quarantine_manifest_s3_url', 'quarantine_file_s3_url') if key in record})
        except Exception as audit_error:
            record.update(status='quarantine_failed', audit_error_type=type(audit_error).__name__)
        logging.error('FILE REJECTED: filename=%s stage=%s status=%s error_type=%s manifest=%s',
                      item['filename'], stage, record['status'], record['error_type'],
                      record.get('quarantine_manifest_s3_url'))
        return record

    @task
    @tracer.start_as_current_span(f"{dag_name}_task_download_zip")
    def download_zip_files(file_list: List[str]):
        with TemporaryDirectory() as folder:
            def download(item):
                filename = item['filename']
                remote_file = f"{config['source_path'].rstrip('/')}/{filename}"
                local_file = os.path.join(folder, filename)

                manifest_path = None
                if config['require_sender_manifest']:
                    manifest_path = os.path.join(folder, filename + '.manifest.json')

                    def retrieve_manifest():
                        source = _hook(config['source_connection_type'], config['source_connection'])
                        try:
                            conn = source.get_conn()
                            path = remote_file + '.manifest.json'
                            size = (conn.stat(path).st_size if config['source_connection_type'] == 'SFTP'
                                    else conn.size(path))
                            if size <= 0 or size > config['manifest_max_bytes']:
                                raise ValueError('Sender manifest size outside allowed limit')
                            source.retrieve_file(path, manifest_path)
                            if os.path.getsize(manifest_path) != size:
                                raise ConnectionError('Manifest download size mismatch')
                        finally:
                            source.close_conn()

                    _retry(retrieve_manifest, f'Manifest download {filename}')
                    with open(manifest_path, encoding='utf-8-sig') as stream:
                        manifest = validate_sender_manifest(json.load(stream), filename, config['file_txt'])
                    item['manifest'] = manifest
                    item['zip_integrity'] = {key: manifest['archive'][key] for key in ('size', 'sha256')}
                    _audit(item, 'RECEIVED', 'download_zip')

                else:
                    _audit(item, 'RECEIVED', 'download_zip')

                def retrieve():
                    source = _hook(config['source_connection_type'], config['source_connection'])
                    try:
                        remote_conn = source.get_conn()

                        def snapshot():
                            if config['source_connection_type'] == 'SFTP':
                                attrs = remote_conn.stat(remote_file)
                                return attrs.st_size, attrs.st_mtime
                            return remote_conn.size(remote_file), None

                        # The sender publishes .done last; it must still be present.
                        if config['require_sender_manifest']:
                            if config['source_connection_type'] == 'SFTP':
                                remote_conn.stat(remote_file + '.done')
                            else:
                                remote_conn.size(remote_file + '.done')
                        before = snapshot()
                        source.retrieve_file(remote_file, local_file)
                        after = snapshot()
                        fingerprint = fingerprint_file(local_file)
                        if before != after:
                            raise ValueError(f'Source changed during download: {filename}')
                        if fingerprint['size'] != before[0]:
                            raise ConnectionError(f'Download size mismatch: {filename}')
                        if item.get('zip_integrity') is not None:
                            verify_fingerprint(fingerprint, item['zip_integrity'], filename)
                        validate_zip(local_file)
                        return fingerprint
                    finally:
                        source.close_conn()

                fingerprint = _retry(retrieve, f'SFTP/FTP download {filename}')
                url = _upload_result_to_s3(local_file, config['input_s3_app'], datetime.now())
                if manifest_path is not None:
                    _upload_result_to_s3(manifest_path, config['input_s3_app'], datetime.now())
                item['zip_integrity'] = fingerprint
                _audit(item, 'VERIFIED', 'download_zip')
                return [{**item, 'zip_s3_url': url, 'zip_integrity': fingerprint}]

            return process_isolated(
                [{'filename': name, 'zipfilename': name, 'source_path': config['source_path']}
                 for name in file_list],
                download, lambda item, error: _quarantine(
                    item, 'download_zip', error, os.path.join(folder, item['filename'])))

    @task
    @tracer.start_as_current_span(f"{dag_name}_task_extract_d4")
    def extract_d4_files(files):
        with TemporaryDirectory() as folder:
            def extract(item):
                local_zip = _download_result_from_s3(
                    item['zip_s3_url'], folder, item['zip_integrity'])
                validate_zip(local_zip)
                results = []
                with zipfile.ZipFile(local_zip) as archive:
                    members = [member for member in archive.infolist()
                               if fnmatch.fnmatchcase(os.path.basename(member.filename).upper(),
                                                     config['file_txt'].upper())]
                    if not members:
                        raise ValueError(f"No D4_NPG.TXT found in {item['filename']}")
                    names = [os.path.basename(member.filename).upper() for member in members]
                    if len(names) != len(set(names)):
                        raise ValueError(f"Duplicate D4 basename in {item['filename']}")
                    expected_files = {entry['filename'].upper(): entry
                                      for entry in item.get('manifest', {}).get('files', [])}
                    if item.get('manifest') is not None and set(names) != set(expected_files):
                        raise ValueError('D4 ZIP members do not match sender manifest')
                    for member in members:
                        filename = os.path.basename(member.filename)
                        d4 = {'filename': filename, 'zipfilename': item['filename'],
                              'zip_s3_url': item['zip_s3_url'], 'source_path': config['source_path'],
                              'expected': expected_files.get(filename.upper())}
                        _audit(d4, 'RECEIVED', 'extract_d4')
                        with TemporaryDirectory(dir=folder) as member_folder:
                            target = os.path.join(member_folder, filename)
                            try:
                                fingerprint = extract_verified_member(archive, member, target)
                                if d4['expected'] is not None:
                                    verify_fingerprint(fingerprint, {key: d4['expected'][key]
                                                       for key in ('size', 'sha256')}, filename)
                                d4['d4_integrity'] = fingerprint
                                url = _upload_result_to_s3(
                                    target, config['result_s3_app_rintis'], datetime.now())
                                d4['d4_s3_url'] = url
                                _audit(d4, 'VERIFIED', 'extract_d4')
                                results.append(d4)
                            except Exception as error:
                                results.append(_quarantine(d4, 'extract_d4', error, target))
                return results

            return process_isolated(files, extract, lambda item, error: _quarantine(
                item, 'extract_zip', error, os.path.join(folder, item['filename'])))

    @task
    @tracer.start_as_current_span(f"{dag_name}_task_load_d4")
    def load_d4_files(files):
        def load(item):
            with TemporaryDirectory() as folder:
                local_file = os.path.join(folder, item['filename'])
                try:
                    local_file = _download_result_from_s3(
                        item['d4_s3_url'], folder, item['d4_integrity'])
                    with open(local_file, 'r', encoding=config['txt_encoding'], errors='strict') as stream:
                        report = parse_d4(stream)
                    if item.get('expected') is not None:
                        reconcile_report(report, item['expected'])
                    controls = report['controls']
                    if controls['banks_without_complete_footer']:
                        logging.warning('SOURCE COMPLETENESS UNCONFIRMED: %s missing footer for %s',
                                        item['filename'], controls['banks_without_complete_footer'])
                    _audit(item, 'VALIDATED', 'read_d4',
                           {'transaction_count': sum(g['frekuensi'] for g in report['groups']),
                            'total_amount': str(sum((g['totalamount'] for g in report['groups']), Decimal('0'))),
                            'sender_manifest_verified': item.get('expected') is not None,
                            'report_controls': controls})
                except Exception as error:
                    return [_quarantine(item, 'read_d4', error, local_file)]
            connection = db_rekon.__setconnection_cursor__(config['db_rekon'])
            try:
                result = persist_d4(connection, item, report,
                                    get_current_context()['dag_run'].run_id)
            finally:
                connection.close()
            logging.info('D4 LOAD RESULT: %s', result)
            return [{**item, **result}]

        return process_isolated(files, load, lambda item, error: _quarantine(item, 'database', error))

    @task
    def show_result(files):
        failures = [item for item in files
                    if item.get('status') in ('quarantined', 'quarantine_failed')]
        for item in files:
            logging.info('FILE RESULT: filename=%s status=%s stage=%s manifest=%s',
                         item['filename'], item.get('status'), item.get('stage'),
                         item.get('quarantine_manifest_s3_url'))
        if failures:
            raise RuntimeError(f'{len(failures)} file(s) rejected; see FILE RESULT and quarantine manifests')
        return {'processed': len(files), 'rejected': 0}

    files = get_unprocessed_files()
    zip_files = download_zip_files(files)
    d4_files = extract_d4_files(zip_files)
    loaded_files = load_d4_files(d4_files)
    show_result(loaded_files)


dag_settlement_off_us()
