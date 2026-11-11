"""Exercise merchant transfer callables against the existing local Docker services.

Run from the project root:
    docker exec -i airflow-web python - < scripts/test_merchant_migration_local.py
    docker exec -i airflow-web python - --dag-run < scripts/test_merchant_migration_local.py

By default this checks actual task callables. --dag-run creates a real three-task
local DagRun using DAG.test(), without the database follow-up tasks. Neither mode
executes SSH BO commands or imports merchants. Existing pending database batches
are never processed by this check.
"""
from contextlib import closing, ExitStack
from datetime import datetime, timezone
import hashlib
import importlib.util
import json
import os
import sys
from pathlib import Path
import uuid
from unittest.mock import Mock, patch
from urllib.parse import urlsplit

from airflow.sdk import BaseHook
from airflow.models.connection import Connection
from airflow.settings import Session
from dags.utils.hooks.S3HookOcbc import S3HookOcbc
from dags.utils.hooks.SFTPHookOcbc import SFTPHookOcbc


PROJECT_DATA = Path('/opt/airflow/project-data')
DAG_FILE = Path('/opt/airflow/dags/payment-and-collection-platform/acquiring/dag_merchant_migration.py')
SAMPLE = PROJECT_DATA / 'ACQ_MTI/ClearingRintis/PWC_OCBC_MERCHANT_20261111104150.txt'
EXPECTED_TASKS = {
    'get_files_from_server_sftp', 'put_files_to_server_bo', 'delete_files_source',
    'get_files_to_process', 'update_merchant_to_db',
}


def load_local_connections():
    """Expose allowlisted ORM rows to SDK hooks in this standalone test process."""
    local_hosts = {
        'SFTP_EBSP': {'sftp-pwc', 'airflow-sftp-pwc', 'localhost', '127.0.0.1'},
        'SFTP_PWC_POST': {'sftp-pwc', 'airflow-sftp-pwc', 'localhost', '127.0.0.1'},
        'db_acq_psql': {'postgres', 'airflow-postgres', 'localhost', '127.0.0.1'},
    }
    with closing(Session()) as session:
        rows = {row.conn_id: row for row in session.query(Connection).filter(
            Connection.conn_id.in_([*local_hosts, 's3-artefact'])).all()}
        missing = {*local_hosts, 's3-artefact'} - rows.keys()
        if missing:
            raise RuntimeError('Missing local connection IDs: ' + ', '.join(sorted(missing)))
        for connection_id, allowed_hosts in local_hosts.items():
            if (rows[connection_id].host or '').lower() not in allowed_hosts:
                raise RuntimeError(f'{connection_id} does not point to an allowed local service')
        extra = rows['s3-artefact'].extra_dejson
        endpoint = extra.get('endpoint_url') or extra.get('service_config', {}).get('s3', {}).get('endpoint_url')
        if not endpoint or urlsplit(endpoint).hostname not in {'s3-local', 'airflow-s3-local', 'localhost', '127.0.0.1'}:
            raise RuntimeError('s3-artefact does not point to an allowed local S3 endpoint')
        # Airflow SDK connection lookup outside a task cannot use the metastore
        # directly. Credentials remain in this process environment and are not logged.
        for connection_id, connection in rows.items():
            os.environ['AIRFLOW_CONN_' + connection_id.upper()] = connection.get_uri()


def require_local(connection_id, allowed_hosts):
    connection = BaseHook.get_connection(connection_id)
    if (connection.host or '').lower() not in allowed_hosts:
        raise RuntimeError(f'{connection_id} does not point to an allowed local service')
    return connection


def fingerprint(stream):
    digest = hashlib.sha256()
    size = 0
    while chunk := stream.read(1024 * 1024):
        digest.update(chunk)
        size += len(chunk)
    return {'sha256': digest.hexdigest(), 'bytes': size}


def main():
    dag_run_mode = '--dag-run' in sys.argv[1:]
    run_id = 'merchant_local_' + datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '_' + uuid.uuid4().hex[:8]
    report_directory = PROJECT_DATA / 'artifacts/merchant_migration-local' / run_id
    report_directory.mkdir(parents=True)
    report = {
        'run_id': run_id, 'status': 'running',
        'scope': ('Real three-task local DagRun via DAG.test(), excluding database follow-up'
                  if dag_run_mode else
                  'Actual task callables and local SFTP/S3 services; not an Airflow scheduled/test DagRun'),
        'ssh_executed': False, 'powercard_import_tested': False,
        'database': 'not_executed', 'tasks': {},
    }
    try:
        load_local_connections()
        local_sftp_hosts = {'sftp-pwc', 'airflow-sftp-pwc', 'localhost', '127.0.0.1'}
        require_local('SFTP_EBSP', local_sftp_hosts)
        require_local('SFTP_PWC_POST', local_sftp_hosts)
        s3_connection = BaseHook.get_connection('s3-artefact')
        s3_extra = s3_connection.extra_dejson
        endpoint = s3_extra.get('endpoint_url') or s3_extra.get('service_config', {}).get('s3', {}).get('endpoint_url')
        if not endpoint or urlsplit(endpoint).hostname not in {'s3-local', 'airflow-s3-local', 'localhost', '127.0.0.1'}:
            raise RuntimeError('s3-artefact does not point to an allowed local S3 endpoint')
        bucket = s3_extra.get('bucket')
        if not bucket:
            raise RuntimeError('s3-artefact must provide bucket in connection extra')

        spec = importlib.util.spec_from_file_location('merchant_migration_local_test', DAG_FILE)
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        dag = module.merchant_migration()
        if set(dag.task_ids) != EXPECTED_TASKS:
            raise AssertionError(f'Unexpected active task set: {dag.task_ids}')
        report['active_task_ids'] = sorted(dag.task_ids)
        remote_base = '/upload/merchant_migration_test/' + run_id
        source_path, destination_path = remote_base + '/source', remote_base + '/destination'
        module.config.update(
            source_connection='SFTP_EBSP', source_path=source_path,
            destination_connection='SFTP_PWC_POST', destination_path=destination_path,
            s3_conn_id='s3-artefact', acq_conn_id='db_acq_psql',
        )
        with SAMPLE.open('rb') as stream:
            original = fingerprint(stream)
        source_hook = SFTPHookOcbc(ssh_conn_id='SFTP_EBSP', use_managed_conn=False)
        with closing(source_hook.get_conn()) as sftp:
            source_hook.create_directory(source_path)
            with SAMPLE.open('rb') as source, sftp.open(source_path + '/' + SAMPLE.name, 'wb') as target:
                while chunk := source.read(1024 * 1024):
                    target.write(chunk)
        report['source_sample'] = str(SAMPLE)
        report['source_fingerprint'] = original
        report['staging_source_path'] = source_path
        report['destination_path'] = destination_path

        # The module's SSH constructor must never be used. SFTP's own SSH transport
        # remains available; it cannot execute the BO application command.
        guards = []
        execution_run_id = run_id
        with ExitStack() as stack:
            for constructor in ('SSHHook', 'SSHOperator'):
                if hasattr(module, constructor):
                    forbidden_ssh = Mock(side_effect=AssertionError('SSH BO execution is disabled for this test'))
                    stack.enter_context(patch.object(module, constructor, forbidden_ssh))
                    guards.append(forbidden_ssh)
            if dag_run_mode:
                transfer_dag = dag.partial_subset(
                    task_ids=['get_files_from_server_sftp', 'put_files_to_server_bo', 'delete_files_source'],
                    include_downstream=False, include_upstream=True)
                dag_run = transfer_dag.test(logical_date=datetime.now(timezone.utc))
                execution_run_id = dag_run.run_id
                report['airflow_run_id'] = execution_run_id
                report['airflow_dag_id'] = dag_run.dag_id
                report['airflow_state'] = str(dag_run.state)
                states = {ti.task_id: str(ti.state) for ti in dag_run.get_task_instances()}
                report['tasks'].update(states)
                if set(states) != {'get_files_from_server_sftp', 'put_files_to_server_bo', 'delete_files_source'}:
                    raise AssertionError('Unexpected tasks executed by the transfer DagRun')
                if any(state != 'success' for state in states.values()) or str(dag_run.state) != 'success':
                    raise AssertionError('Transfer DagRun did not finish successfully')
            else:
                files = dag.get_task('get_files_from_server_sftp').python_callable(run_id=run_id)
                if files != [SAMPLE.name]:
                    raise AssertionError(f'Unexpected downloaded filenames: {files}')
                report['tasks']['get_files_from_server_sftp'] = files
                uploaded = dag.get_task('put_files_to_server_bo').python_callable(files=files, run_id=run_id)
                if uploaded != files:
                    raise AssertionError('Upload did not return the complete batch')
                report['tasks']['put_files_to_server_bo'] = uploaded
                deleted = dag.get_task('delete_files_source').python_callable(files=uploaded, run_id=run_id)
                if deleted != uploaded:
                    raise AssertionError('Source deletion did not return the complete batch')
                report['tasks']['delete_files_source'] = deleted
            for forbidden_ssh in guards:
                forbidden_ssh.assert_not_called()

        with closing(SFTPHookOcbc(ssh_conn_id='SFTP_PWC_POST').get_conn()) as sftp:
            with sftp.open(destination_path + '/' + SAMPLE.name, 'rb') as stream:
                destination = fingerprint(stream)
        with closing(SFTPHookOcbc(ssh_conn_id='SFTP_EBSP').get_conn()) as sftp:
            if sftp.listdir(source_path):
                raise AssertionError('Source staging directory must be empty after deletion')
        key = '/'.join((module.config['s3_prefix'].rstrip('/'), module.workflow_name, execution_run_id, SAMPLE.name))
        s3 = S3HookOcbc(aws_conn_id='s3-artefact').get_conn()
        with closing(s3.get_object(Bucket=bucket, Key=key)['Body']) as stream:
            artefact = fingerprint(stream)
        with SAMPLE.open('rb') as stream:
            original_after = fingerprint(stream)
        if not original == destination == artefact == original_after:
            raise AssertionError('Original, S3 artefact, and destination fingerprints differ')
        report.update(s3_url=f's3://{bucket}/{key}', destination_fingerprint=destination,
                      s3_fingerprint=artefact, source_staging_deleted=True, original_preserved=True)

        # Polling a prior real SUCCESS batch is read-only. Do not run its update
        # task, because this test must not mutate unrelated enrichment records.
        try:
            require_local('db_acq_psql', {'postgres', 'airflow-postgres', 'localhost', '127.0.0.1'})
            with closing(module.db_rekon.__setconnection_cursor__(connection_id='db_acq_psql')) as connection:
                with closing(connection.cursor()) as cursor:
                    cursor.execute("SELECT to_regclass('merchant_migration_process_log_tt')")
                    control_table_exists = cursor.fetchone()[0] is not None
            if control_table_exists:
                pending = dag.get_task('get_files_to_process').python_callable()
                report['tasks']['get_files_to_process'] = {'pending_count': len(pending)}
                if not pending:
                    result = dag.get_task('update_merchant_to_db').python_callable(files=[])
                    report['tasks']['update_merchant_to_db'] = result
                    report['database'] = 'polling succeeded; no pending files; update was a no-op'
                else:
                    report['database'] = 'polling succeeded; existing pending batches left untouched'
            else:
                report['database'] = 'not executed: local control table does not exist'
        except Exception as error:
            report['database'] = 'not completed: ' + type(error).__name__
        report['status'] = 'transfer_passed'
    except Exception as error:
        report['status'] = 'failed'
        report['error_type'] = type(error).__name__
        raise
    finally:
        report_path = report_directory / 'report.json'
        report_path.write_text(json.dumps(report, indent=2) + '\n')
        print('REPORT ' + str(report_path))
        print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()
