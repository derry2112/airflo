"""PostgreSQL utility with explicitly selected Airflow connections."""

from airflow.providers.postgres.hooks.postgres import PostgresHook


class PostgresEtlHook:
    def read_polar_with_exec(self, sql, connection_id, params=None):
        hook = PostgresHook(postgres_conn_id=connection_id)
        return hook.get_records(sql=sql, parameters=params)
