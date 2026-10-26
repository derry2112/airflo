"""Oracle utility with explicitly selected Airflow connections."""

from dags.utils.base_sql import BaseSQL


class OracleEtlHook:
    def read_polar_with_exec(self, sql, connection_id, params=None):
        return BaseSQL().read_polar_with_exec(
            sql=sql, connection_id=connection_id, params=params
        ).rows()
