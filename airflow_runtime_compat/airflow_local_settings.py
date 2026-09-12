"""Opt-in local compatibility layer; enabled only by compose.compat.yaml."""
import functools
import runpy
from unittest.mock import patch


class LegacyDBConnection:
    def __init__(self, type='postgres', connection_id=None,
                 workflow_name=None, chunksize=None, mssql_conn_id=None):
        if type != 'postgres':
            raise ValueError('Local legacy adapter requires type=postgres')
        if not connection_id:
            raise ValueError('connection_id is required')
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        self.hook = PostgresHook(postgres_conn_id=connection_id)

    def execute_fetch_many(self, sql, params=None):
        # Legacy query uses SQLAlchemy-style :rrns; psycopg2 needs %(rrns)s.
        if params is not None and 'rrns' in params:
            sql = sql.replace(':rrns', '%(rrns)s')
        return self.hook.get_records(sql=sql, parameters=params)

    def get_data(self, sql, params=None):
        return self.execute_fetch_many(sql, params)


_existing = runpy.run_path('/opt/airflow/runtime/airflow_local_settings.py')


def task_policy(task):
    original_policy = _existing.get('task_policy')
    if original_policy:
        original_policy(task)
    if task.dag_id != 'etl_OCBC_MTI_RINTIS_ProcessFileRintisQRRecon':
        return
    original = getattr(task, 'python_callable', None)
    if original is None:
        return

    @functools.wraps(original)
    def local_callable(*args, **kwargs):
        from data_payment.core.controller import Operators_Acquiring
        with patch.object(Operators_Acquiring, 'DBConnection', LegacyDBConnection):
            return original(*args, **kwargs)

    task.python_callable = local_callable
