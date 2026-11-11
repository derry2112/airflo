"""Database adapter contracts without importing Airflow on the host."""
import ast
from contextlib import closing
from pathlib import Path
import re
from types import SimpleNamespace
import unittest
from unittest.mock import Mock


DAG_PATH = Path(__file__).resolve().parents[1] / 'dags/payment-and-collection-platform/acquiring/dag_merchant_migration.py'
HELPERS = {'_pg', '_insert_bashscript_log', '_get_merchant_powercard_new', '_delete_merchant_powercard'}


def load_helpers():
    tree = ast.parse(DAG_PATH.read_text())
    nodes = [node for node in ast.walk(tree)
             if isinstance(node, ast.FunctionDef) and node.name in HELPERS]
    namespace = {
        'closing': closing,
        'config': {'acq_conn_id': 'test-acquiring', 'merchantapps_conn_id': 'test-merchantapps',
                   'pwc_conn_id': 'test-powercard'},
    }
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(DAG_PATH), 'exec'), namespace)
    return namespace


class MerchantMigrationDatabaseTest(unittest.TestCase):
    def setUp(self):
        self.namespace = load_helpers()
        self.cursor = Mock(rowcount=3)
        self.connection = Mock()
        self.connection.cursor.return_value = self.cursor
        self.connect = Mock(return_value=self.connection)
        self.namespace['db_rekon'] = SimpleNamespace(__setconnection_cursor__=self.connect)
        self.oracle = Mock()
        self.namespace['db_pwc'] = self.oracle

    def test_control_write_commits_and_returns_affected_rows(self):
        params = {'status': 'SUCCESS', 'filename': 'merchant.txt'}
        result = self.namespace['_pg']('UPDATE process_log SET status=%(status)s WHERE filename=%(filename)s', params)
        self.assertEqual(result, 3)
        self.connect.assert_called_once_with(connection_id='test-acquiring')
        self.cursor.execute.assert_called_once_with(
            'UPDATE process_log SET status=%(status)s WHERE filename=%(filename)s', params)
        self.connection.commit.assert_called_once_with()
        self.connection.rollback.assert_not_called()
        self.cursor.close.assert_called_once_with()
        self.connection.close.assert_called_once_with()

    def test_enrichment_reads_use_separate_connection_and_keep_rows(self):
        self.cursor.fetchall.return_value = [('merchant-1',), ('merchant-2',)]
        result = self.namespace['_pg']('SELECT mid_pwc FROM enrichment WHERE filename=%(filename)s',
                                       {'filename': 'merchant.txt'}, merchantapps=True, fetch=True)
        self.assertEqual(result, [('merchant-1',), ('merchant-2',)])
        self.connect.assert_called_once_with(connection_id='test-merchantapps')
        self.cursor.fetchone.assert_not_called()
        self.connection.commit.assert_called_once_with()
        self.connection.close.assert_called_once_with()

    def test_insert_returning_keeps_log_id(self):
        self.cursor.fetchone.return_value = (42,)
        log_id = self.namespace['_insert_bashscript_log']('migration', 'run-1', 'command', 'migration')
        self.assertEqual(log_id, 42)
        sql, params = self.cursor.execute.call_args.args
        self.assertIn('RETURNING id', sql)
        self.assertEqual(params, {'dags_name': 'migration', 'run_id': 'run-1',
                                  'command_script': 'command', 'operation': 'migration'})
        self.cursor.fetchone.assert_called_once_with()
        self.cursor.fetchall.assert_not_called()
        self.connection.commit.assert_called_once_with()

    def test_failed_write_rolls_back_and_closes_resources(self):
        self.cursor.execute.side_effect = ValueError('database rejected write')
        with self.assertRaisesRegex(ValueError, 'database rejected write'):
            self.namespace['_pg']('UPDATE process_log SET status=%(status)s', {'status': 'SUCCESS'})
        self.connection.commit.assert_not_called()
        self.connection.rollback.assert_called_once_with()
        self.cursor.close.assert_called_once_with()
        self.connection.close.assert_called_once_with()

    def test_oracle_lookup_binds_each_mid_and_keeps_result_shape(self):
        rows = [('merchant-1', 'AP1', 'AP2', 'SUCCESS', '04', None)]
        self.oracle.read_polar_with_exec.return_value = rows
        merchant_ids = ['merchant-1', "merchant-2' OR '1'='1"]
        result = self.namespace['_get_merchant_powercard_new'](merchant_ids)
        self.assertIs(result, rows)
        self.oracle.read_polar_with_exec.assert_called_once()
        call = self.oracle.read_polar_with_exec.call_args.kwargs
        self.assertEqual(call['connection_id'], 'test-powercard')
        self.assertEqual(call['params'], {'m0': merchant_ids[0], 'm1': merchant_ids[1]})
        self.assertIn('IN (:m0, :m1)', call['sql'])
        self.assertNotIn(merchant_ids[1], call['sql'])

    def test_empty_oracle_lookup_does_not_connect(self):
        self.assertEqual(self.namespace['_get_merchant_powercard_new']([]), [])
        self.oracle.read_polar_with_exec.assert_not_called()

    def test_cleanup_keeps_foreign_key_order_in_one_bound_execution(self):
        merchant_id = "merchant-1' OR '1'='1"
        self.namespace['_delete_merchant_powercard'](merchant_id)
        self.oracle.read_polar_with_exec.assert_called_once()
        call = self.oracle.read_polar_with_exec.call_args.kwargs
        self.assertEqual(call['connection_id'], 'test-powercard')
        self.assertEqual(call['params'], {'mid': merchant_id})
        sql = call['sql'].strip()
        self.assertTrue(sql.upper().startswith('BEGIN'))
        self.assertTrue(sql.upper().endswith('END;'))
        self.assertNotIn(merchant_id, sql)
        # Children are removed before their parent rows, with merchant last.
        targets = re.findall(r'DELETE\s+FROM\s+(\w+)', sql, flags=re.IGNORECASE)
        self.assertEqual(targets, [
            'mer_contract_caracteristics', 'mer_product_context', 'accepted_currencies',
            'mer_accepted_networks', 'mer_acc_contract_elmt_cre', 'mer_acc_contract_transactions',
            'mer_acc_contract_transactions', 'mer_acceptance_contract_elmt', 'mer_charges',
            'mer_acc_contract_services', 'mer_fees_event_periodic', 'mer_acceptor_point',
            'cre_merchant_data', 'mer_acquiring_site', 'mer_activity', 'accounts_link',
            'accounts_link', 'mer_account', 'address_details', 'address_list',
            'mer_representative', 'merchant',
        ])
        self.assertNotIn('COMMIT', sql.upper())

    def test_oracle_cleanup_errors_propagate(self):
        self.oracle.read_polar_with_exec.side_effect = RuntimeError('cleanup rejected')
        with self.assertRaisesRegex(RuntimeError, 'cleanup rejected'):
            self.namespace['_delete_merchant_powercard']('merchant-1')


if __name__ == '__main__':
    unittest.main()
