"""Oracle utility with explicitly selected Airflow connections."""

from airflow.providers.oracle.hooks.oracle import OracleHook


class OracleEtlHook:
    def read_polar_with_exec(self, sql, connection_id, params=None):
        hook = OracleHook(oracle_conn_id=connection_id)
        return hook.get_records(sql=sql, parameters=params)
