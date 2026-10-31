import ast
import hashlib
import logging
import time
import json
import uuid
import fnmatch
from copy import deepcopy
from urllib.parse import urlsplit
from typing import List
from datetime import timezone
import io
import os
import shutil
import zipfile
from tempfile import TemporaryDirectory
import re
from datetime import datetime
from types import ModuleType, SimpleNamespace
from pathlib import Path
import unittest
from decimal import Decimal
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
# Load the pure parser/persistence definitions without importing Airflow on the host.
DAG_PATH = ROOT / 'dags/payment-and-collection-platform/acquiring/dag_settlement_off_us.py'
tree = ast.parse(DAG_PATH.read_text())
names = {'DETAIL_FIELDS', '_FIRST', '_SECOND', 'parse_d4', 'persist_d4',
         'fingerprint_stream', 'fingerprint_file', 'verify_fingerprint',
         'validate_zip', 'extract_verified_member', 'retry_transfer', 'process_isolated',
         'is_transient_transfer_error', 'select_ready_archives', 'validate_sender_manifest',
         'reconcile_report', 'write_ingestion_audit', 'source_is_stable'}
nodes = []
for node in ast.walk(tree):
    if isinstance(node, ast.FunctionDef) and node.name in names:
        node = deepcopy(node)
        node.decorator_list = []
        nodes.append(node)
config_node = next(node for node in tree.body if isinstance(node, ast.Assign)
                   and any(isinstance(target, ast.Name) and target.id == 'config' for target in node.targets))
parser_config = {ast.literal_eval(key): ast.literal_eval(value)
                 for key, value in zip(config_node.value.keys, config_node.value.values)
                 if isinstance(key, ast.Constant) and key.value in
                 {'detail_fields', 'first_line_pattern', 'second_line_pattern'}}
d4 = ModuleType('dag_settlement_off_us')
d4.__dict__.update(re=re, datetime=datetime, Decimal=Decimal, hashlib=hashlib,
                   zipfile=zipfile, os=os, shutil=shutil, Path=Path, logging=logging,
                   time=time, json=json, fnmatch=fnmatch, config=parser_config)
exec(compile(ast.Module(body=nodes, type_ignores=[]), str(DAG_PATH), 'exec'), d4.__dict__)

class FrozenDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        return cls(2026, 10, 31, 12, tzinfo=tz)

HEADER = '''ACQUIRER: BC-BANK CENTRAL ASIA
TANGGAL : 25-09-23
ISSUER : NS-BANK OCBC NISP
'''
TRANSACTION = ''' 1 23/09/23 10:26:12 10 0210 9999990000000001 75,000.00 277.50 758696 000057
 000000000168 D2AC2130 7230-885002005973 KUMAWI EYELASH BOGOR
'''


def expected_entry(content, name='251030D4_NPG.txt', report=None):
    report = report or d4.parse_d4(content.decode('utf-8-sig').splitlines())
    return {'filename': name, **d4.fingerprint_stream(io.BytesIO(content)),
            'transaction_count': sum(g['frekuensi'] for g in report['groups']),
            'total_amount': str(sum((g['totalamount'] for g in report['groups']), Decimal('0.00'))),
            'banks': [{'acquirer': g['issuer'], 'period': g['period'].isoformat(),
                       'transaction_count': g['frekuensi'], 'total_amount': str(g['totalamount'])}
                      for g in report['groups']]}


def sender_manifest(payload, entries, name='input.zip'):
    return {'schema_version': 1, 'archive': {'filename': name,
            **d4.fingerprint_stream(io.BytesIO(payload))}, 'files': entries}


class ParseTests(unittest.TestCase):
    def test_multiple_pages_same_bank_and_other_empty_bank(self):
        text = HEADER + TRANSACTION + HEADER + TRANSACTION + HEADER.replace(
            'BC-BANK CENTRAL ASIA', 'AJ-360001 ARTAJASA') + '0 SALES 0 0.00\n'
        report = d4.parse_d4(text.splitlines())
        self.assertEqual(len(report['groups']), 2)
        bca, aj = report['groups']
        self.assertEqual(bca['frekuensi'], 2)
        self.assertEqual(bca['totalamount'], Decimal('150000.00'))
        self.assertIsNone(bca['institutionid'])
        self.assertEqual(aj['institutionid'], 360001)
        self.assertEqual(aj['frekuensi'], 0)
        self.assertEqual(bca['details'][0]['refno'], '000000000168')
        self.assertEqual(bca['details'][0]['merchantname'], 'KUMAWI EYELASH BOGOR')

    def test_periods_remain_separate(self):
        report = d4.parse_d4((HEADER + TRANSACTION + HEADER.replace(
            '25-09-23', '26-09-23') + TRANSACTION).splitlines())
        self.assertEqual(len(report['groups']), 2)

    def test_incomplete_rejected_before_next_header_and_at_eof(self):
        first = TRANSACTION.splitlines()[0] + '\n'
        for suffix in ('', HEADER):
            with self.subTest(suffix=suffix), self.assertRaisesRegex(ValueError, 'line 4'):
                d4.parse_d4((HEADER + first + suffix).splitlines())

    def test_bad_amount_and_missing_section_rejected(self):
        for text in (HEADER + TRANSACTION.replace('75,000.00', 'invalid'), TRANSACTION, ''):
            with self.subTest(text=text), self.assertRaises(ValueError):
                d4.parse_d4(text.splitlines())


class IntegrityTests(unittest.TestCase):
    def test_same_size_corruption_and_truncation_detected(self):
        expected = d4.fingerprint_stream(io.BytesIO(b'original'))
        for data in (b'originAL', b'orig'):
            with self.subTest(data=data), self.assertRaisesRegex(ValueError, 'SHA-256 mismatch'):
                d4.verify_fingerprint(d4.fingerprint_stream(io.BytesIO(data)), expected, 'D4')

    def test_stream_read_error_propagates(self):
        stream = MagicMock()
        stream.read.side_effect = [b'partial', OSError('read failed')]
        with self.assertRaisesRegex(OSError, 'read failed'):
            d4.fingerprint_stream(stream)

    def test_zip_truncation_and_crc_corruption_rejected(self):
        with TemporaryDirectory() as folder:
            path = Path(folder) / 'report.zip'
            with zipfile.ZipFile(path, 'w', compression=zipfile.ZIP_STORED) as archive:
                archive.writestr('reportD4_NPG.txt', b'unique-payload-for-crc')
            valid = path.read_bytes()
            changed = valid.replace(b'unique-payload-for-crc', b'unique-payload-for-CRC')
            self.assertNotEqual(changed, valid)
            for data in (valid[:-22], changed, b'not a zip'):
                path.write_bytes(data)
                with self.subTest(data_size=len(data)), self.assertRaises((ValueError, zipfile.BadZipFile)):
                    d4.validate_zip(path)

    def test_extract_and_digest_match_original(self):
        content = (HEADER + TRANSACTION).encode()
        with TemporaryDirectory() as folder:
            path = Path(folder) / 'report.zip'
            target = Path(folder) / 'report.txt'
            with zipfile.ZipFile(path, 'w', zipfile.ZIP_DEFLATED) as archive:
                archive.writestr('reportD4_NPG.txt', content)
            d4.validate_zip(path)
            with zipfile.ZipFile(path) as archive:
                actual = d4.extract_verified_member(archive, archive.infolist()[0], target)
            self.assertEqual(target.read_bytes(), content)
            d4.verify_fingerprint(actual, d4.fingerprint_stream(io.BytesIO(content)), 'TXT')

    def test_empty_member_rejected(self):
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, 'w') as archive:
            archive.writestr('reportD4_NPG.txt', b'')
        with TemporaryDirectory() as folder, zipfile.ZipFile(buffer) as archive:
            with self.assertRaisesRegex(ValueError, 'Empty D4'):
                d4.extract_verified_member(archive, archive.infolist()[0], Path(folder) / 'out')

    def test_corrupt_member_not_left_on_disk(self):
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, 'w', zipfile.ZIP_STORED) as archive:
            archive.writestr('reportD4_NPG.txt', b'original')
        broken = io.BytesIO(buffer.getvalue().replace(b'original', b'originAL'))
        with TemporaryDirectory() as folder, zipfile.ZipFile(broken) as archive:
            target = Path(folder) / 'out'
            with self.assertRaises(zipfile.BadZipFile):
                d4.extract_verified_member(archive, archive.infolist()[0], target)
            self.assertFalse(target.exists())

    def test_unreadable_text_rejected(self):
        with io.TextIOWrapper(io.BytesIO((HEADER + TRANSACTION).encode() + b'\xff'),
                              encoding='utf-8-sig', errors='strict') as stream:
            with self.assertRaises(UnicodeDecodeError):
                d4.parse_d4(stream)
        with self.assertRaisesRegex(ValueError, 'NUL byte'):
            d4.parse_d4((HEADER + TRANSACTION + '\x00').splitlines())


class RecoveryTests(unittest.TestCase):
    def test_retry_then_recovery(self):
        operation = MagicMock(side_effect=[OSError('disconnect'), TimeoutError('timeout'), 'ok'])
        self.assertEqual(d4.retry_transfer(operation, 'test', delay_seconds=0), 'ok')
        self.assertEqual(operation.call_count, 3)

    def test_exhausted_retry_raises_original_failure(self):
        operation = MagicMock(side_effect=OSError('offline'))
        with self.assertRaisesRegex(OSError, 'offline'):
            d4.retry_transfer(operation, 'test', delay_seconds=0)
        self.assertEqual(operation.call_count, 3)

    def test_isolation_preserves_failures_and_processes_remaining_files(self):
        failed = {'filename': 'old', 'status': 'quarantined'}
        operation = MagicMock(side_effect=[ValueError('bad'), [{'filename': 'good', 'status': 'inserted'}]])
        rejected = {'filename': 'bad', 'status': 'quarantined'}
        on_failure = MagicMock(return_value=rejected)
        result = d4.process_isolated([failed, {'filename': 'bad'}, {'filename': 'good'}], operation, on_failure)
        self.assertEqual(result, [failed, rejected, {'filename': 'good', 'status': 'inserted'}])
        self.assertEqual(operation.call_count, 2)

    def test_mixed_zip_keeps_valid_file_and_quarantines_truncated_txt(self):
        # Exercise actual nested extract/load/quarantine tasks with in-memory S3.
        namespace = dict(d4.__dict__)
        namespace.update(json=json, uuid=uuid, fnmatch=fnmatch, timezone=timezone,
                         TemporaryDirectory=TemporaryDirectory, List=List,
                         workflow_name='test', dag_name='test',
                         get_current_context=lambda: {'dag_run': type('Run', (), {'run_id': 'test'})()},
                         datetime=FrozenDateTime, tz=timezone.utc,
                         config={**parser_config, 'source_path': '/inbound/', 'file_txt': '*D4_NPG.txt',
                                 'result_s3_app_rintis': 'test', 'txt_encoding': 'utf-8-sig', 'db_rekon': 'test'})
        outer = next(node for node in tree.body
                     if isinstance(node, ast.FunctionDef) and node.name == 'dag_settlement_off_us')
        functions = [deepcopy(node) for node in outer.body if isinstance(node, ast.FunctionDef)]
        for node in functions:
            node.decorator_list = []
        exec(compile(ast.Module(body=functions, type_ignores=[]), str(DAG_PATH), 'exec'), namespace)
        storage = {}
        def upload(path, app, date):
            url = 's3://test/' + app + '/' + Path(path).name
            storage[url] = Path(path).read_bytes()
            return url
        def download(url, folder, expected):
            path = Path(folder) / url.rsplit('/', 1)[-1]
            path.write_bytes(storage[url])
            d4.verify_fingerprint(d4.fingerprint_file(path), expected, url)
            return str(path)
        namespace['_audit'] = MagicMock()
        namespace['_upload_result_to_s3'] = upload
        namespace['_download_result_from_s3'] = download
        namespace['db_rekon'] = SimpleNamespace(__setconnection_cursor__=MagicMock(return_value=MagicMock()))
        persist = MagicMock(return_value={'status': 'inserted', 'fileid': 1})
        namespace['persist_d4'] = persist
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, 'w') as archive:
            archive.writestr('261031D4_NPG.txt', HEADER + TRANSACTION)
            archive.writestr('261031BAD_D4_NPG.txt', HEADER + TRANSACTION.splitlines()[0] + '\n')
        payload = buffer.getvalue()
        storage['s3://test/input.zip'] = payload
        extracted = namespace['extract_d4_files']([{
            'filename': 'input.zip', 'zip_s3_url': 's3://test/input.zip',
            'zip_integrity': d4.fingerprint_stream(io.BytesIO(payload)),
            'manifest': sender_manifest(payload, [
                expected_entry((HEADER + TRANSACTION).encode(), '261031D4_NPG.txt'),
                expected_entry((HEADER + TRANSACTION.splitlines()[0] + '\n').encode(),
                               '261031BAD_D4_NPG.txt', d4.parse_d4((HEADER + TRANSACTION).splitlines()))])}])
        result = namespace['load_d4_files'](extracted)
        self.assertEqual([item['status'] for item in result], ['inserted', 'quarantined'])
        self.assertEqual(persist.call_count, 1)
        rejected = result[1]
        self.assertIn('incomplete transaction', rejected['reason'])
        manifest = json.loads(storage[rejected['quarantine_manifest_s3_url']])
        self.assertEqual(manifest['stage'], 'read_d4')
        self.assertEqual(storage[rejected['quarantine_file_s3_url']],
                         (HEADER + TRANSACTION.splitlines()[0] + '\n').encode())
        with self.assertRaisesRegex(RuntimeError, '1 file'):
            namespace['show_result'](result)

        zip_only = namespace['extract_d4_files']([{
            'filename': 'input.zip', 'zip_s3_url': 's3://test/input.zip',
            'zip_integrity': d4.fingerprint_stream(io.BytesIO(payload))}])
        self.assertTrue(all(item['expected'] is None for item in zip_only))
        zip_only_result = namespace['load_d4_files'](zip_only)
        self.assertEqual([item['status'] for item in zip_only_result], ['inserted', 'quarantined'])

        namespace['_upload_result_to_s3'] = MagicMock(side_effect=OSError('S3 unavailable'))
        failed = namespace['_quarantine']({'filename': 'bad'}, 'read_d4', ValueError('bad input'))
        self.assertEqual(failed['status'], 'quarantine_failed')
        self.assertNotIn('quarantine_manifest_s3_url', failed)



class S3ArtefactUploadTests(unittest.TestCase):
    def setUp(self):
        outer = next(node for node in tree.body
                     if isinstance(node, ast.FunctionDef) and node.name == 'dag_settlement_off_us')
        upload = deepcopy(next(node for node in outer.body
                               if isinstance(node, ast.FunctionDef) and node.name == '_upload_once'))
        self.utility = MagicMock()
        self.client = MagicMock()
        namespace = dict(d4.__dict__, urlsplit=urlsplit,
                         S3ArtefactHook=MagicMock(return_value=self.utility),
                         S3HookOcbc=MagicMock(return_value=SimpleNamespace(get_conn=lambda: self.client)),
                         get_current_context=lambda: {'dag_run': SimpleNamespace(run_id='test-run')})
        exec(compile(ast.Module(body=[upload], type_ignores=[]), str(DAG_PATH), 'exec'), namespace)
        self.upload = namespace['_upload_once']

    def test_existing_utility_and_readback_verification(self):
        content = b'test upload bytes'
        self.utility.put_file_to_s3.return_value = 's3://test/artefacts/input.txt'
        body = io.BytesIO(content)
        self.client.get_object.return_value = {'Body': body}
        with TemporaryDirectory() as folder:
            path = Path(folder) / 'input.txt'
            path.write_bytes(content)
            result = self.upload(str(path), 'settlement', datetime(2026, 10, 31))
            self.assertEqual(result, 's3://test/artefacts/input.txt')
            self.assertTrue(path.exists())
            kwargs = self.utility.put_file_to_s3.call_args.kwargs
            self.assertEqual(kwargs['workflow_name'], 'test-run/input.txt')
            self.assertEqual(kwargs['date_file'], '20261031')
            self.assertIsNone(kwargs['ext'])
            self.assertFalse(kwargs['is_delete_immediately'])
        self.assertTrue(body.closed)
        self.client.upload_file.assert_not_called()

    def test_swallowed_failure_does_not_continue(self):
        with TemporaryDirectory() as folder:
            path = Path(folder) / 'input.txt'
            path.write_bytes(b'input')
            for url in ('', None, 'not-an-s3-url'):
                self.utility.put_file_to_s3.return_value = url
                with self.subTest(url=url), self.assertRaises((ConnectionError, ValueError)):
                    self.upload(str(path), 'settlement', datetime(2026, 10, 31))
        self.client.get_object.assert_not_called()

    def test_readback_mismatch_fails(self):
        self.utility.put_file_to_s3.return_value = 's3://test/input.txt'
        body = io.BytesIO(b'changed')
        self.client.get_object.return_value = {'Body': body}
        with TemporaryDirectory() as folder:
            path = Path(folder) / 'input.txt'
            path.write_bytes(b'original')
            with self.assertRaisesRegex(ValueError, 'SHA-256 mismatch'):
                self.upload(str(path), 'settlement', datetime(2026, 10, 31))
        self.assertTrue(body.closed)


class ContractTests(unittest.TestCase):
    def setUp(self):
        self.entry = expected_entry((HEADER + TRANSACTION).encode())
        self.manifest = sender_manifest(b'ZIP', [self.entry])

    def test_zip_only_is_default(self):
        self.assertEqual(d4.select_ready_archives(['input.zip'], 'input.zip'), (['input.zip'], []))

    def test_stability_and_minimum_age(self):
        self.assertTrue(d4.source_is_stable((100, 100), (100, 100), 140, 30))
        self.assertFalse(d4.source_is_stable((100, 100), (101, 100), 140, 30))
        self.assertFalse(d4.source_is_stable((100, 100), (100, 120), 140, 30))
        self.assertFalse(d4.source_is_stable((100, 120), (100, 120), 140, 30))
        self.assertFalse(d4.source_is_stable((0, 100), (0, 100), 140, 30))

    def test_footer_checks_and_missing_footer_visibility(self):
        report = d4.parse_d4((HEADER + TRANSACTION + '0 SALES 1 75,000.00\n').splitlines())
        self.assertEqual(report['controls']['validated_footers'], 1)
        self.assertEqual(report['controls']['banks_without_complete_footer'], [])
        for footer in ('0 SALES 2 75,000.00', '0 SALES 1 70,000.00'):
            with self.subTest(footer=footer), self.assertRaisesRegex(ValueError, 'footer count/amount mismatch'):
                d4.parse_d4((HEADER + TRANSACTION + footer).splitlines())
        report = d4.parse_d4((HEADER + TRANSACTION).splitlines())
        self.assertEqual(len(report['controls']['banks_without_complete_footer']), 1)

    def test_wait_until_both_sidecars_exist(self):
        for names, expected in [(['input.zip'], []),
                                (['input.zip', 'input.zip.done'], []),
                                (['input.zip', 'input.zip.done', 'input.zip.manifest.json'], ['input.zip'])]:
            ready, waiting = d4.select_ready_archives(names, 'input.zip', require_sidecars=True)
            self.assertEqual(ready, expected)
            self.assertEqual(len(waiting), int(not expected))

    def test_manifest_and_reconciliation(self):
        d4.validate_sender_manifest(self.manifest, 'input.zip')
        d4.reconcile_report(d4.parse_d4((HEADER + TRANSACTION).splitlines()), self.entry)

    def test_invalid_contract_rejected(self):
        changes = [lambda m: m.update(schema_version=2),
                   lambda m: m['archive'].update(filename='other.zip'),
                   lambda m: m['archive'].update(sha256='wrong'),
                   lambda m: m['files'][0].update(filename='../badD4_NPG.txt'),
                   lambda m: m['files'][0].update(transaction_count=True),
                   lambda m: m['files'][0].update(total_amount=75000.0),
                   lambda m: m['files'][0]['banks'][0].update(transaction_count=2)]
        for change in changes:
            manifest = deepcopy(self.manifest)
            change(manifest)
            with self.subTest(change=change), self.assertRaises(ValueError):
                d4.validate_sender_manifest(manifest, 'input.zip')

    def test_loss_of_complete_transaction_is_detected(self):
        report = d4.parse_d4((HEADER + TRANSACTION + TRANSACTION).splitlines())
        with self.assertRaisesRegex(ValueError, 'Reconciliation'):
            d4.reconcile_report(report, self.entry)

    def test_invalid_input_is_not_retried(self):
        for error in (ValueError('bad checksum'), zipfile.BadZipFile('bad ZIP'),
                      PermissionError('forbidden'), FileNotFoundError('missing')):
            operation = MagicMock(side_effect=error)
            with self.subTest(error=error), self.assertRaises(type(error)):
                d4.retry_transfer(operation, 'test', delay_seconds=0)
            self.assertEqual(operation.call_count, 1)

    def test_backoff_increases(self):
        operation = MagicMock(side_effect=[TimeoutError(), ConnectionError(), 'ok'])
        with patch.object(d4.time, 'sleep') as sleep:
            self.assertEqual(d4.retry_transfer(operation, 'test'), 'ok')
        self.assertEqual([call.args[0] for call in sleep.call_args_list], [2, 4])


class PersistenceTests(unittest.TestCase):
    def setUp(self):
        self.report = d4.parse_d4((HEADER + TRANSACTION).splitlines())
        self.connection = MagicMock()
        self.cursor = self.connection.cursor.return_value.__enter__.return_value
        self.cursor.fetchone.side_effect = [None, (11,), (22,)]
        self.item = {'filename': '251030D4_NPG.txt', 'source_path': '/inbound/',
                     'expected': expected_entry((HEADER + TRANSACTION).encode()),
                     'd4_integrity': d4.fingerprint_stream(io.BytesIO((HEADER + TRANSACTION).encode()))}

    def test_ids_and_atomic_commit(self):
        result = d4.persist_d4(self.connection, self.item, self.report, 'test')
        self.assertEqual(result['fileid'], 11)
        self.assertEqual(result['details'], 1)
        self.connection.commit.assert_called_once()
        self.connection.rollback.assert_not_called()
        sql, rows = self.cursor.executemany.call_args.args
        self.assertIn('sw_rekon.acqpwc_d4detail_tt', sql)
        self.assertEqual(rows[0][0], 22)
        self.assertEqual(len(rows[0]), 14)

    def test_duplicate_skips_inserts(self):
        self.cursor.fetchone.side_effect = [(11, self.item['d4_integrity']['sha256'], 1)]
        result = d4.persist_d4(self.connection, self.item, self.report, 'test')
        self.assertEqual(result['status'], 'already_processed')
        self.cursor.executemany.assert_not_called()
        self.assertEqual(self.cursor.execute.call_count, 3)

    def test_detail_failure_rolls_back_file_and_summary(self):
        self.cursor.executemany.side_effect = RuntimeError('database failure')
        with self.assertRaises(RuntimeError):
            d4.persist_d4(self.connection, self.item, self.report, 'test')
        self.connection.rollback.assert_called_once()
        self.connection.commit.assert_not_called()

    def test_changed_content_requires_approval(self):
        self.cursor.fetchone.side_effect = [(11, 'a' * 64, 1), None]
        with self.assertRaisesRegex(ValueError, 'approved reprocess'):
            d4.persist_d4(self.connection, self.item, self.report, 'run')
        self.connection.rollback.assert_called_once()
        self.cursor.executemany.assert_not_called()

    def test_approved_correction_keeps_old_version(self):
        self.cursor.fetchone.side_effect = [(11, 'a' * 64, 1), (7,), (12,), (22,)]
        result = d4.persist_d4(self.connection, self.item, self.report, 'run')
        self.assertEqual(result['version'], 2)
        self.assertEqual(result['fileid'], 12)
        statements = [call.args[0] for call in self.cursor.execute.call_args_list]
        self.assertTrue(any('isactive=FALSE' in sql for sql in statements))
        self.assertTrue(any("status='APPLIED'" in sql for sql in statements))
        self.assertFalse(any(sql.startswith('DELETE') for sql in statements))

    def test_reconciliation_fails_before_any_insert(self):
        self.item['expected']['banks'][0]['total_amount'] = '1.00'
        with self.assertRaisesRegex(ValueError, 'Reconciliation'):
            d4.persist_d4(self.connection, self.item, self.report, 'run')
        self.cursor.execute.assert_not_called()


if __name__ == '__main__':
    unittest.main()
