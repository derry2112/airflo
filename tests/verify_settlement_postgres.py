"""Local PostgreSQL integration verification in a disposable schema.

Run inside the local Airflow container via stdin (see the technical notes).
Never uses the settlement tables in sw_rekon. Cleans up its own schema.
"""
import ast
from copy import deepcopy
from datetime import datetime
from decimal import Decimal
import fnmatch
import hashlib
import json
import logging
import os
from pathlib import Path
import re
import shutil
import time
from urllib.parse import urlsplit
import uuid
import zipfile

import psycopg2


def main():
    dsn = os.environ['AIRFLOW_CONN_DB_REKON_UAT']
    if urlsplit(dsn).hostname not in ('postgres', 'localhost', '127.0.0.1'):
        raise RuntimeError('This verification only supports the local PostgreSQL environment')
    schema = 'settlement_verify_' + uuid.uuid4().hex
    root = Path('/opt/airflow/dags')
    connection = psycopg2.connect(dsn)
    try:
        for filename in ('rekon.sql', 'settlement_ingestion_controls.sql'):
            sql = (root / 'sql' / filename).read_text().replace('sw_rekon', schema)
            with connection.cursor() as cursor:
                cursor.execute(sql)
            connection.commit()
        path = root / 'payment-and-collection-platform/acquiring/dag_settlement_off_us.py'
        tree = ast.parse(path.read_text().replace('sw_rekon', schema))
        names = {'parse_d4', 'persist_d4', 'write_ingestion_audit', 'reconcile_report'}
        nodes = [node for node in ast.walk(tree)
                 if isinstance(node, ast.FunctionDef) and node.name in names]
        config_node = next(node for node in tree.body if isinstance(node, ast.Assign)
                           and any(isinstance(target, ast.Name) and target.id == 'config'
                                   for target in node.targets))
        parser_config = {ast.literal_eval(key): ast.literal_eval(value)
                         for key, value in zip(config_node.value.keys, config_node.value.values)
                         if isinstance(key, ast.Constant) and key.value in
                         {'detail_fields', 'first_line_pattern', 'second_line_pattern'}}
        namespace = dict(globals(), config=parser_config)
        exec(compile(ast.Module(body=nodes, type_ignores=[]), str(path), 'exec'), namespace)
        parse, persist = namespace['parse_d4'], namespace['persist_d4']
        text = ('ACQUIRER: BC-BANK CENTRAL ASIA\nTANGGAL : 25-09-23\n'
                'ISSUER : NS-BANK OCBC NISP\n'
                ' 1 23/09/23 10:26:12 10 0210 9999990000000001 75,000.00 277.50 758696 000057\n'
                ' 000000000168 D2AC2130 7230-885002005973 TEST MERCHANT\n'
                '0 SALES 1 75,000.00\n')
        item = {'filename': '251030D4_NPG.txt', 'source_path': '/test/', 'expected': None,
                'd4_integrity': {'sha256': hashlib.sha256(text.encode()).hexdigest()}}
        report = parse(text.splitlines())
        result = persist(connection, item, report, 'integration')
        assert result['version'] == 1 and result['details'] == 1
        assert persist(connection, item, report, 'rerun')['status'] == 'already_processed'

        changed = deepcopy(item)
        changed['d4_integrity']['sha256'] = 'b' * 64
        try:
            persist(connection, changed, report, 'unapproved')
        except ValueError as error:
            assert 'approved reprocess' in str(error)
        else:
            raise AssertionError('Unapproved correction was accepted')
        with connection.cursor() as cursor:
            cursor.execute(f'INSERT INTO {schema}.acqpwc_reprocess_request_tt '
                           '(filename, previoussha256, expectedsha256, approvedby, reason) '
                           'VALUES (%s,%s,%s,%s,%s)',
                           (item['filename'], item['d4_integrity']['sha256'], 'b' * 64,
                            'integration_operator', 'test approved correction'))
        connection.commit()
        corrected = persist(connection, changed, report, 'approved')
        assert corrected['version'] == 2
        with connection.cursor() as cursor:
            cursor.execute(f'SELECT fileversion,isactive FROM {schema}.acqpwc_file_log_tt ORDER BY fileversion')
            assert cursor.fetchall() == [(1, False), (2, True)]
            cursor.execute(f'SELECT status FROM {schema}.acqpwc_reprocess_request_tt')
            assert cursor.fetchone()[0] == 'APPLIED'
            cursor.execute(f'SELECT count(*) FROM {schema}.acqpwc_d4detail_tt')
            assert cursor.fetchone()[0] == 2
        connection.commit()

        try:
            with connection.cursor() as cursor:
                cursor.execute(f'INSERT INTO {schema}.acqpwc_file_log_tt '
                               "(filepath,filename,transactiontype,fileversion) VALUES ('/test/',%s,'D4',3)",
                               (item['filename'].upper(),))
        except psycopg2.IntegrityError as error:
            assert error.pgcode == '23505'
            connection.rollback()
        else:
            raise AssertionError('Unique constraint did not reject duplicate active filename')

        broken_report = deepcopy(report)
        broken_report['groups'][0]['details'][0]['cardno'] = '9' * 61
        broken_item = {**item, 'filename': '251031D4_NPG.txt'}
        try:
            persist(connection, broken_item, broken_report, 'rollback')
        except psycopg2.DataError:
            pass
        else:
            raise AssertionError('Expected detail-column failure')
        with connection.cursor() as cursor:
            cursor.execute(f'SELECT count(*) FROM {schema}.acqpwc_file_log_tt WHERE filename=%s',
                           (broken_item['filename'],))
            assert cursor.fetchone()[0] == 0
            cursor.execute(f'SELECT count(*) FROM {schema}.acqpwc_ingestion_audit_tt '
                           "WHERE runid='rollback' AND status='LOADED'")
            assert cursor.fetchone()[0] == 0
        connection.commit()

        with connection.cursor() as cursor:
            cursor.execute(f'INSERT INTO {schema}.acqpwc_reprocess_request_tt '
                           '(filename, previoussha256, expectedsha256, approvedby, reason) '
                           'VALUES (%s,%s,%s,%s,%s)',
                           (item['filename'], 'b' * 64, 'c' * 64, 'integration_operator', 'test rollback'))
        connection.commit()
        try:
            persist(connection, {**changed, 'd4_integrity': {'sha256': 'c' * 64}},
                    broken_report, 'correction_rollback')
        except psycopg2.DataError:
            pass
        else:
            raise AssertionError('Correction should fail on invalid detail')
        with connection.cursor() as cursor:
            cursor.execute(f'SELECT fileversion,isactive FROM {schema}.acqpwc_file_log_tt ORDER BY fileversion')
            assert cursor.fetchall() == [(1, False), (2, True)]
            cursor.execute(f'SELECT status FROM {schema}.acqpwc_reprocess_request_tt WHERE expectedsha256=%s',
                           ('c' * 64,))
            assert cursor.fetchone()[0] == 'APPROVED'
            cursor.execute(f'SELECT status FROM {schema}.acqpwc_ingestion_audit_tt ORDER BY auditid')
            assert [row[0] for row in cursor.fetchall()] == ['LOADED', 'ALREADY_PROCESSED', 'LOADED']
        connection.commit()
        print('PASS: actual PostgreSQL load, duplicate detection, unique constraint, approved versioning, '
              'audit, and rollback including correction rollback; production-schema data untouched.')
    finally:
        connection.rollback()
        with connection.cursor() as cursor:
            cursor.execute(f'DROP SCHEMA IF EXISTS {schema} CASCADE')
        connection.commit()
        connection.close()


if __name__ == '__main__':
    main()
