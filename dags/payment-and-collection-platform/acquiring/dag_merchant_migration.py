import logging
import os
import shlex
import stat
from contextlib import closing
from datetime import datetime, timezone, timedelta
from airflow.sdk import BaseHook, dag, task
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from utils.postgres import PostgresEtlHook
from dags.utils.oracle import OracleEtlHook
from dags.utils.hooks.S3HookOcbc import S3HookOcbc
from dags.utils.hooks.SFTPHookOcbc import SFTPHookOcbc
from dags.utils.callback import Callback
from dags.utils.models.airflow_constant import Owner

logger = logging.getLogger(__name__)
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
db_pwc = OracleEtlHook()

config = {
    "owner": Owner.PAYMENT_AND_COLLECTION_PLATFORM,
    'email_list': [],
    'start_date': '2026-07-26',
    'schedule_interval': None,
    'tags': ['data_payment', 'acquiring', 'merchant_migration'],
    'workflow_name': 'etl_MERCHANT_MIGRATION_PWC_SIT',
    'acq_conn_id': 'postgres-db-acquiring',
    'merchantapps_conn_id': 'DB_MERCHANTAPPS',
    'pwc_conn_id': 'oracle-db-sit-pwc',
    'source_connection': 'sftp-pten',
    'source_path': '/mtiuat/toocbc/powercard/migration',
    'destination_connection': 'sftp-pwc',
    'destination_path': '/pwrcard/home/usr/data/merchant_migration',
    'success_folder': '/pwrcard/home/usr/data/merchant_migration/process_success',
    'failed_folder': '/pwrcard/home/usr/data/merchant_migration/process_failed',
    'inprogress_folder': '/pwrcard/home/usr/data/merchant_migration/inprogress',
    # Original staging path retained as configuration; transfers stream through S3.
    'local_path': '/data/airflow/nfs/artifacts/data_payment/acquiring/merchant_migration/',
    'ssh_connection': 'ssh-pwc-bo',
    's3_conn_id': 's3-artefact',
    's3_prefix': 'artefacts/data_payment/merchant_migration',
}

FILE_PATH = os.path.dirname(os.path.realpath(__file__))
workflow_name = config["workflow_name"]
dag_name = workflow_name
callback = Callback(email_list=config["email_list"])

user_cron = config["schedule_interval"]
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
    description="Transfer file merchant melalui S3; logic SSH disimpan tanpa task aktif",
    default_args=default_args,
    start_date=datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=tz),
    catchup=False,
    dagrun_timeout=timedelta(minutes=10),
    tags=dynamic_tags,
    max_active_runs=1,
    on_success_callback=callback.dag_success_alert,
)

def merchant_migration():
    @task
    def get_files_from_server_sftp(**context):
        return _get_files_from_server_sftp(
            config['source_connection'], config['source_path'], local_path=config['local_path'], **context)

    @task
    def put_files_to_server_bo(files, **context):
        return _put_files_to_server_bo(
            config['destination_connection'], config['destination_path'],
            local_path=config['local_path'], filenames=files, **context)

    @task
    def delete_files_source(files, **context):
        return _delete_files_source(
            config['source_connection'], config['source_path'], filenames=files, **context)

    def process_files_remote(files, **context):
        # Logic retained for BO integration; deliberately not an Airflow task.
        for filename in files:
            _insert_merchant_migration_process(filename, config['workflow_name'])
        successful = _process_files_remote(
            config['ssh_connection'], config['destination_path'],
            success_folder=config['success_folder'], failed_folder=config['failed_folder'],
            inprogress_folder=config['inprogress_folder'], filenames=files, **context)
        for filename in files:
            _update_merchant_migration_process(
                'SUCCESS' if filename in successful else 'FAILED',
                None if filename in successful else 'Bash script failed', filename)
        return successful

    @task
    def get_files_to_process():
        return _get_files_to_process()

    @task
    def update_merchant_to_db(files):
        return _update_merchant_to_db(filenames=files)


    def _pg(sql, params=None, *, merchantapps=False, fetch=False, one=False):
        connection_id = config['merchantapps_conn_id'] if merchantapps else config['acq_conn_id']
        with closing(db_rekon.__setconnection_cursor__(connection_id=connection_id)) as conn, closing(conn.cursor()) as cursor:
            try:
                cursor.execute(sql, params)
                result = (cursor.fetchone() if one else cursor.fetchall()) if fetch else cursor.rowcount
                conn.commit()
                return result
            except Exception:
                conn.rollback()
                raise

    def _files(kwargs):
        ti = kwargs['ti']
        upstream = list(ti.task.upstream_task_ids)
        if not upstream:
            return []
        if len(upstream) != 1:
            raise ValueError('Specify filenames explicitly when a task has multiple upstream tasks')
        return ti.xcom_pull(task_ids=upstream[0]) or []

    def _s3():
        conn_id = config.get('s3_conn_id', 's3-artefact')
        bucket = config.get('s3_bucket') or BaseHook.get_connection(conn_id).extra_dejson.get('bucket')
        if not bucket:
            raise ValueError('Set s3_bucket or bucket in S3 connection extra')
        return S3HookOcbc(aws_conn_id=conn_id).get_conn(), bucket

    def _key(filename, kwargs):
        prefix = config.get('s3_prefix', 'artefacts/data_payment/merchant_migration')
        return '/'.join((prefix.rstrip('/'), workflow_name, kwargs['run_id'], filename))

    def _get_files_from_server_sftp(ssh_conn_id, remote_path, local_path=None, **kwargs):
        client, bucket = _s3()
        downloaded = []
        with closing(SFTPHookOcbc(ssh_conn_id=ssh_conn_id).get_conn()) as sftp:
            for entry in sftp.listdir_attr(remote_path.rstrip('/')):
                if not stat.S_ISREG(entry.st_mode):
                    continue
                try:
                    with sftp.open(f"{remote_path.rstrip('/')}/{entry.filename}", 'rb') as source:
                        client.upload_fileobj(source, bucket, _key(entry.filename, kwargs))
                    downloaded.append(entry.filename)
                except Exception:
                    logger.exception('[%s] failed to stream to S3', entry.filename)
        return downloaded

    def _put_files_to_server_bo(ssh_conn_id, remote_path, local_path=None, filenames=None, **kwargs):
        """S3 -> BO SFTP; only completed files are passed to the next task.

        Original method body (lines 52–76) was not present in the supplied images.
        """
        filenames = _files(kwargs) if filenames is None else filenames
        client, bucket = _s3()
        uploaded = []
        hook = SFTPHookOcbc(ssh_conn_id=ssh_conn_id, use_managed_conn=False)
        with closing(hook.get_conn()) as sftp:
            hook.create_directory(remote_path)
            for filename in filenames:
                try:
                    with closing(client.get_object(Bucket=bucket, Key=_key(filename, kwargs))['Body']) as source:
                        with sftp.open(f"{remote_path.rstrip('/')}/{filename}", 'wb') as target:
                            while chunk := source.read(1024 * 1024):
                                target.write(chunk)
                    uploaded.append(filename)
                except Exception:
                    logger.exception('[%s] failed to stream to BO', filename)
        return uploaded

    def _delete_files_source(ssh_conn_id, remote_path, filenames=None, **kwargs):
        filenames = _files(kwargs) if filenames is None else filenames
        deleted = []
        with closing(SFTPHookOcbc(ssh_conn_id=ssh_conn_id).get_conn()) as sftp:
            for filename in filenames:
                try:
                    sftp.remove(f"{remote_path.rstrip('/')}/{filename}")
                    deleted.append(filename)
                except Exception:
                    logger.exception('[%s] failed to delete', filename)
        return deleted

    def _copy_file_to_parent(ssh_client, remote_path, filename, **kwargs):
        """Replace remote shell cp with BO SFTP -> S3 -> BO SFTP streaming."""
        client, bucket = _s3()
        key = _key(filename, kwargs) + '.parent-copy'
        try:
            with closing(ssh_client.open_sftp()) as sftp:
                with sftp.open(f"{remote_path.rstrip('/')}/{filename}", 'rb') as source:
                    client.upload_fileobj(source, bucket, key)
                with closing(client.get_object(Bucket=bucket, Key=key)['Body']) as source:
                    destination = f"{remote_path.rstrip('/').rsplit('/', 1)[0]}/{filename}"
                    with sftp.open(destination, 'wb') as target:
                        while chunk := source.read(1024 * 1024):
                            target.write(chunk)
            return True
        except Exception:
            logger.exception('[%s] failed to copy to parent', filename)
            return False

    def _process_files_remote(ssh_conn_id, remote_path, success_folder=None,
                             failed_folder=None, inprogress_folder=None, filenames=None, **kwargs):
        filenames = _files(kwargs) if filenames is None else filenames
        if not filenames:
            return []
        parent = remote_path.rstrip('/').rsplit('/', 1)[0]
        date_str = datetime.now().strftime('%d%m%Y')
        successfiles = []
        with closing(SSHHook(ssh_conn_id=ssh_conn_id).get_conn()) as ssh:
            for filename in filenames:
                if not _copy_file_to_parent(ssh, remote_path, filename, **kwargs):
                    logger.info('%s failed to copy file to remote path', filename)
                args = ['./PWROCBC.sh', 'ocbc_acquiring_migration', date_str, filename, 'M'] + ['NULL'] * 8
                cmd = f'cd {shlex.quote(parent)} && {shlex.join(args)}'
                log_id = _insert_bashscript_log(workflow_name, kwargs['run_id'], cmd, 'ocbc_acquiring_migration')
                # Drain both streams before checking exit status (large output must not deadlock).
                _, stdout, stderr = ssh.exec_command(cmd)
                channel = stdout.channel
                out, err = bytearray(), bytearray()
                import time
                while True:
                    while channel.recv_ready():
                        out.extend(channel.recv(65536))
                    while channel.recv_stderr_ready():
                        err.extend(channel.recv_stderr(65536))
                    if channel.exit_status_ready() and not channel.recv_ready() and not channel.recv_stderr_ready():
                        break
                    time.sleep(0.05)
                exit_status = channel.recv_exit_status()
                logger.info('[%s] Bash script output: %s', filename, out.decode(errors='replace'))
                if err:
                    logger.warning('[%s] Bash script stderr: %s', filename, err.decode(errors='replace'))
                if exit_status in (0, 26):
                    successfiles.append(filename)
                    _update_bashscript_log('SUCCESS', exit_status,
                        f'Bash Script Executed Successfully with exit code : {exit_status}', log_id)
                else:
                    error = f'Gagal Proses file [{filename}] Bash Script Failed (exit_status={exit_status})'
                    _update_enrichment_status_failed_by_filename(filename, error)
                    _update_bashscript_log('FAILED', exit_status, error, log_id)
        return successfiles

    def _move_file(ssh_client, src_path, dest_path, filename):
        cmd = 'mv ' + shlex.join([f"{src_path.rstrip('/')}/{filename}", f"{dest_path.rstrip('/')}/{filename}"])
        _, stdout, _ = ssh_client.exec_command(cmd)
        return stdout.channel.recv_exit_status() == 0

    def _insert_merchant_migration_process(filename, workflow_name):
        return _pg("""INSERT INTO merchant_migration_process_log_TT (filename, workflow_name, bash_status)
            VALUES (%(filename)s, %(workflow_name)s, 'PROCESSING') ON CONFLICT (filename) DO NOTHING""",
            dict(filename=filename, workflow_name=workflow_name))

    def _update_merchant_migration_process(status, error, filename):
        return _pg("""UPDATE merchant_migration_process_log_TT SET bash_status=%(status)s,
            bash_processed_at=now(), bash_error=%(error)s WHERE filename=%(filename)s""",
            dict(status=status, error=error, filename=filename))

    def _get_mid_in_progress_by_filename(file_name_pwc):
        return _pg("""SELECT mid_pwc FROM sw_acquiring.enrichment_onboarding_master
            WHERE file_name_pwc=%(file_name_pwc)s AND status='03'""",
            dict(file_name_pwc=file_name_pwc), merchantapps=True, fetch=True)

    def _update_enrichment_status(mid_pwc, acceptor_point, acceptor_point_ipp):
        return _pg("""UPDATE sw_acquiring.enrichment_onboarding_master SET status='04',
            acceptor_point_id=%(acceptor_point)s, acceptor_point_id_ipp=%(acceptor_point_ipp)s
            WHERE mid_pwc=%(mid_pwc)s""", dict(mid_pwc=mid_pwc, acceptor_point=acceptor_point,
            acceptor_point_ipp=acceptor_point_ipp), merchantapps=True)

    def _update_enrichment_status_failed(mid_pwc, error_reason):
        return _pg("""UPDATE sw_acquiring.enrichment_onboarding_master SET status='02',
            error_reason=%(error_reason)s WHERE mid_pwc=%(mid_pwc)s""",
            dict(mid_pwc=mid_pwc, error_reason=error_reason), merchantapps=True)

    def _update_enrichment_status_failed_by_filename(filename, error_reason):
        return _pg("""UPDATE sw_acquiring.enrichment_onboarding_master SET status='02',
            error_reason=%(error_reason)s WHERE filename=%(filename)s""",
            dict(filename=filename, error_reason=error_reason), merchantapps=True)

    def _get_files_to_process(**kwargs):
        return _pg("""SELECT filename FROM merchant_migration_process_log_TT
            WHERE bash_status='SUCCESS' AND update_db_status IS NULL""", fetch=True)

    def _mark_db_updated(filename, status, err):
        return _pg("""UPDATE merchant_migration_process_log_TT SET update_db_status=%(status)s,
            update_db_processed_at=now(), update_db_error=%(err)s WHERE filename=%(filename)s""",
            dict(filename=filename, status=status, err=err))

    def _insert_bashscript_log(dags_name, run_id, command_script, operation):
        return _pg("""INSERT INTO powercard_airflow_bashscript_log
            (dags_name, run_id, command_script, date_start, operation)
            VALUES (%(dags_name)s, %(run_id)s, %(command_script)s, now(), %(operation)s) RETURNING id""",
            dict(dags_name=dags_name, run_id=run_id, command_script=command_script, operation=operation),
            fetch=True, one=True)[0]

    def _update_bashscript_log(status, exit_code, error_message, log_id):
        return _pg("""UPDATE powercard_airflow_bashscript_log SET status=%(status)s, date_end=now(),
            exit_code=%(exit_code)s, error_message=%(error_message)s WHERE id=%(log_id)s""",
            dict(status=status, exit_code=exit_code, error_message=error_message, log_id=log_id))

    def _get_merchant_powercard_new(merchant_number):
        if not isinstance(merchant_number, (list, tuple, set)):
            merchant_number = [merchant_number]
        merchant_number = list(merchant_number)
        if not merchant_number:
            return []
        placeholders = ', '.join(f':m{i}' for i in range(len(merchant_number)))
        statement = f"""
        WITH CTE AS (SELECT DISTINCT
            m.MERCHANT_NUMBER AS mid_pwc,
            ap.mer_acceptor_point_id AS ap_number,
            mac.name AS ipp_status,
            CASE WHEN m.MERCHANT_NUMBER IS NOT NULL
                AND a.MERCHANT_NUMBER IS NOT NULL AND ac.MERCHANT_NUMBER IS NOT NULL
                AND ap.MERCHANT_NUMBER IS NOT NULL AND mr.MERCHANT_NUMBER IS NOT NULL
                AND mas.MERCHANT_NUMBER IS NOT NULL AND al.ENTITY_ID IS NOT NULL
                AND mac.MERCHANT_NUMBER IS NOT NULL THEN 'SUCCESS' ELSE 'FAILED' END AS migration_status,
            CASE WHEN m.MERCHANT_NUMBER IS NOT NULL
                AND a.MERCHANT_NUMBER IS NOT NULL AND ac.MERCHANT_NUMBER IS NOT NULL
                AND ap.MERCHANT_NUMBER IS NOT NULL AND mr.MERCHANT_NUMBER IS NOT NULL
                AND mas.MERCHANT_NUMBER IS NOT NULL AND al.ENTITY_ID IS NOT NULL
                AND mac.MERCHANT_NUMBER IS NOT NULL THEN '04' ELSE '02' END AS migration_status_code,
            NVL(CASE WHEN a.MERCHANT_NUMBER IS NULL THEN 'Bank Account, ' ELSE '' END ||
                CASE WHEN ac.MERCHANT_NUMBER IS NULL THEN 'MCC, ' ELSE '' END ||
                CASE WHEN ap.MERCHANT_NUMBER IS NULL THEN 'Acceptor Point Level, ' ELSE '' END ||
                CASE WHEN mr.MERCHANT_NUMBER IS NULL THEN 'Owner / Contact Level, ' ELSE '' END ||
                CASE WHEN al.ENTITY_ID IS NULL THEN 'Account Level, ' ELSE '' END ||
                CASE WHEN mac.MERCHANT_NUMBER IS NULL THEN 'Acceptance Contract Level, ' ELSE '' END,
                '') AS error_note
        FROM merchant m
        LEFT JOIN mer_account a ON m.MERCHANT_NUMBER=a.MERCHANT_NUMBER
        LEFT JOIN mer_activity ac ON m.MERCHANT_NUMBER=ac.MERCHANT_NUMBER
        LEFT JOIN mer_acceptor_point ap ON m.MERCHANT_NUMBER=ap.MERCHANT_NUMBER
        LEFT JOIN mer_representative mr ON m.MERCHANT_NUMBER=mr.MERCHANT_NUMBER
        LEFT JOIN mer_acquiring_site mas ON m.MERCHANT_NUMBER=mas.MERCHANT_NUMBER
        LEFT JOIN accounts_link al ON m.MERCHANT_NUMBER=al.ENTITY_ID
        LEFT JOIN MER_ACCEPTANCE_CONTRACT_ELMT mac ON ap.contract_elmt_id=mac.contract_elmt_id
        WHERE m.MERCHANT_NUMBER IN ({placeholders}))
        SELECT mid_pwc,
            MAX(CASE WHEN ipp_status='Generic Product' OR ipp_status='UMI Product' THEN ap_number END) AS ap_number_generic,
            MAX(CASE WHEN ipp_status='IPP Product' THEN ap_number END) AS ap_number_ipp,
            MIN(migration_status) AS migration_status,
            MIN(migration_status_code) AS migration_status_code,
            CASE WHEN MIN(error_note) IS NULL THEN NULL
                ELSE 'Data ' || REGEXP_REPLACE(RTRIM(MIN(error_note), ', '), ', ([^,]+)$', ' dan \\1')
                    || ' ada yang tidak sesuai' END AS error_note
        FROM CTE GROUP BY mid_pwc
        """
        return db_pwc.read_polar_with_exec(
            sql=statement, connection_id=config['pwc_conn_id'],
            params={f'm{i}': value for i, value in enumerate(merchant_number)})

    def _delete_merchant_powercard(merchant_number):
        acceptor = '(SELECT contract_elmt_id FROM mer_acceptor_point WHERE merchant_number=:mid)'
        statements = [
            f'DELETE FROM {table} WHERE contract_elmt_id IN {acceptor}'
            for table in ('mer_contract_caracteristics', 'mer_product_context', 'accepted_currencies',
                          'mer_accepted_networks', 'mer_acc_contract_elmt_cre', 'mer_acc_contract_transactions')
        ]
        statements += [
            'DELETE FROM mer_acc_contract_transactions WHERE contract_elmt_id IN '
            '(SELECT contract_elmt_id FROM MER_ACCEPTANCE_CONTRACT_ELMT WHERE merchant_number=:mid)',
            f'DELETE FROM mer_acceptance_contract_elmt WHERE contract_elmt_id IN {acceptor}',
            'DELETE FROM mer_charges WHERE charging_id IN '
            '(SELECT charging_id FROM mer_acc_contract_services WHERE merchant_number=:mid)',
        ]
        statements += [f'DELETE FROM {table} WHERE merchant_number=:mid' for table in (
            'mer_acc_contract_services', 'mer_fees_event_periodic', 'mer_acceptor_point',
            'cre_merchant_data', 'mer_acquiring_site', 'mer_activity')]
        statements += [
            "DELETE FROM accounts_link WHERE entity_code='MT' AND entity_id=:mid AND date_create > SYSDATE - 1",
            "DELETE FROM accounts_link WHERE entity_code='MC' AND entity_id=:mid",
            'DELETE FROM mer_account WHERE merchant_number=:mid',
            'DELETE FROM address_details WHERE details_id IN (SELECT address_id FROM address_list '
            'WHERE list_id IN (SELECT address_list_id FROM merchant WHERE merchant_number=:mid))',
            'DELETE FROM address_list WHERE list_id IN '
            '(SELECT address_list_id FROM merchant WHERE merchant_number=:mid)',
            'DELETE FROM mer_representative WHERE merchant_number=:mid',
            'DELETE FROM merchant WHERE merchant_number=:mid',
        ]
        # Execute the ordered cleanup as one transaction through the shared hook.
        # Its provider commits only after the whole PL/SQL block succeeds.
        sql = 'BEGIN\n' + ';\n'.join(statements) + ';\nEND;'
        db_pwc.read_polar_with_exec(
            sql=sql, connection_id=config['pwc_conn_id'], params={'mid': merchant_number})

    def _update_merchant_to_db(filenames=None, **kwargs):
        files = _files(kwargs) if filenames is None else filenames
        counts = dict(success_file_count=0, failed_file_count=0, success_mid_count=0, failed_mid_count=0)
        if not files:
            return 'no files to process'
        for file_name_pwc in files:
            if isinstance(file_name_pwc, (list, tuple)):
                file_name_pwc = file_name_pwc[0]
            try:
                mids = _get_mid_in_progress_by_filename(file_name_pwc)
                if not mids:
                    _mark_db_updated(file_name_pwc, 'SUCCESS', 'No mid in progress found, nothing to update')
                    counts['success_file_count'] += 1
                    continue
                mid_list = [row[0] for row in mids]
                found_map = {row[0]: row for row in _get_merchant_powercard_new(mid_list)}
                for mid in mid_list:
                    if mid in found_map:
                        try:
                            row = found_map[mid]
                            if row[3] == 'SUCCESS':
                                _update_enrichment_status(row[0], row[1], row[2])
                                counts['success_mid_count'] += 1
                            else:
                                _update_enrichment_status_failed(row[0], row[5])
                                _delete_merchant_powercard(row[0])
                                counts['failed_mid_count'] += 1
                        except Exception as exc:
                            _update_enrichment_status_failed(mid, f'Migrasi gagal: {mid} dengan error {exc}')
                            _delete_merchant_powercard(mid)
                            counts['failed_mid_count'] += 1
                    else:
                        _update_enrichment_status_failed(mid, 'Migrasi gagal - periksa kelengkapan data merchant')
                        counts['failed_mid_count'] += 1
                counts['success_file_count'] += 1
            except Exception as exc:
                error = f'Migrasi gagal - file {file_name_pwc} gagal di proses dengan error : {exc}'
                logger.exception(error)
                _update_enrichment_status_failed_by_filename(file_name_pwc, error)
                counts['failed_file_count'] += 1
        logger.info('Finish update_merchant_to_db: %s', counts)
        return counts

    downloaded = get_files_from_server_sftp()
    uploaded = put_files_to_server_bo(downloaded)
    deleted = delete_files_source(uploaded)
    # SSH remains logic only. Poll only files already recorded as bash SUCCESS.
    pending = get_files_to_process()
    deleted >> pending
    update_merchant_to_db(pending)


merchant_migration()
