"""Run the DAG's generator against local databases, without scheduling or sending files."""
import ast
from contextlib import contextmanager
from io import BytesIO
from urllib.parse import urlsplit
import ftplib
import paramiko
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from copy import deepcopy
from datetime import datetime, timezone, timedelta
import logging
import os
from pathlib import Path
import sys
from types import SimpleNamespace
from typing import List, NamedTuple

import boto3
import oracledb
import psycopg2
from botocore.config import Config

sys.path.insert(0, '/opt/airflow')
from dags.data_payment.core.model.Models_ReconPWC import ReconHeader, ReconRecordData

logging.basicConfig(level=logging.WARNING)
path = Path('/opt/airflow/dags/payment-and-collection-platform/acquiring/dag_postingfile.py')
tree = ast.parse(path.read_text())
config_node = next(n for n in tree.body if isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == 'config' for t in n.targets))
ns = dict(globals())
exec(compile(ast.Module(body=[config_node], type_ignores=[]), str(path), 'exec'), ns)
config = ns['config']
ns['posting_settings'] = deepcopy(config['posting_settings'])
ns['logger'] = logging.getLogger('postingfile-local')

class Database:
    def __init__(self, connection):
        self.connection = connection

    def read_polar_with_exec(self, sql, connection_id=None, params=None):
        with self.connection.cursor() as cursor:
            cursor.execute(sql, params or {})
            return cursor.fetchall()

oracle = oracledb.connect(user='POWERCARD', password='powercard', dsn='oracle:1521/FREEPDB1')
way4 = psycopg2.connect(host='postgres', dbname='db_rekon_uat', user='airflow', password='airflow')
enrichment = psycopg2.connect(host='postgres', dbname='db_acq_psql', user='airflow', password='airflow')
ns.update(db_source=Database(oracle), db_way4=Database(way4), db_enrichment=Database(enrichment))
dag = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'dag_postingfile')
helpers = [n for n in dag.body if isinstance(n, ast.FunctionDef) and n.name.startswith('_')]
sender = next(n for n in dag.body if isinstance(n, ast.FunctionDef) and n.name == 'send_file')
sender.decorator_list = []
exec(compile(ast.Module(body=helpers + [sender], type_ignores=[]), str(path), 'exec'), ns)
folder = Path('/opt/airflow/project-data/artifacts/postingfile-local') / datetime.now().strftime('%Y%m%d_%H%M%S')
folder.mkdir(parents=True)
source = Path('/opt/airflow/project-data/ACQ_MTI/ClearingRintisQRIS/QR_RECON_360002_93600028_20261006_ISS')
mti = folder / 'mti' / source.name
pwc = folder / 'pwc' / source.name
mti.parent.mkdir()
pwc.parent.mkdir()
try:
    records = ns['_get_way4_data_new'](save_result_file=False)
    posting_records = ns['_map_way4_to_posting_records_new'](records)
    files = ns['_split_rintis_qr_recon'](str(source), str(mti), str(pwc), posting_records)
    if not files:
        raise RuntimeError('Generator produced no posting files')
    client = boto3.client('s3', endpoint_url='http://s3-local:9000', aws_access_key_id='localposting', aws_secret_access_key='localposting-dev-secret', region_name='us-east-1', config=Config(s3={'addressing_style': 'path'}))
    for result in (mti, pwc, *map(Path, files)):
        key = 'generated/' + str(result.relative_to(folder.parent))
        with result.open('rb') as stream:
            client.upload_fileobj(stream, 'postingfile', key)
        with client.get_object(Bucket='postingfile', Key=key)['Body'] as stream:
            assert stream.read() == result.read_bytes()
        print(f'GENERATED {result} ({result.stat().st_size} bytes); s3://postingfile/{key}')
    for result in map(Path, files):
        print('POSTING RECORDS:', [(line[:2], len(line)) for line in result.read_text().splitlines()])
    if '--send' in sys.argv:
        class LocalFTP:
            def __init__(self):
                self.connection = ftplib.FTP('ftp')
                self.connection.login('airflow', 'airflow')
            def get_conn(self):
                return self.connection
            def close_conn(self):
                self.connection.quit()
        class LocalSFTP:
            def __init__(self, **kwargs):
                self.transport = paramiko.Transport(('sftp-pwc', 22))
                self.transport.connect(username='airflow', password='airflow')
                self.connection = paramiko.SFTPClient.from_transport(self.transport)
            def get_conn(self):
                return self.connection
            def close_conn(self):
                self.connection.close()
                self.transport.close()
        ns['_hook'] = lambda *_: LocalFTP()
        ns['SFTPHookOcbc'] = LocalSFTP
        mti_ref = ns['_stage_result'](str(mti), folder.name, 'mti')
        posting_refs = [ns['_stage_result'](f, folder.name, 'posting') for f in files]
        ns['send_file']([{'filename': source.name, 'mti_local_filepath': mti_ref,
                         'posting_local_filepaths': posting_refs}])
        ftp = LocalFTP()
        try:
            received = BytesIO()
            remote = config['destination_path'] + source.name
            ftp.get_conn().retrbinary('RETR ' + remote, received.write)
            assert received.getvalue() == mti.read_bytes()
            ftp.get_conn().voidcmd('TYPE I')
            assert ftp.get_conn().size(remote + '.chk') == 0
            print('PASS S3 -> FTP:', remote, '(content and marker verified)')
        finally:
            ftp.close_conn()
        sftp = LocalSFTP()
        try:
            for f in files:
                remote = config['target_path'] + Path(f).name
                with sftp.get_conn().open(remote, 'rb') as received:
                    assert received.read() == Path(f).read_bytes()
                print('PASS S3 -> SFTP:', remote, '(content verified)')
        finally:
            sftp.close_conn()
finally:
    oracle.close()
    way4.close()
    enrichment.close()
