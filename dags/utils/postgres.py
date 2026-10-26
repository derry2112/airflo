"""Postgres query hook with optional Polars DataFrame results."""

from contextlib import closing

import polars as pl
from airflow.providers.postgres.hooks.postgres import PostgresHook


class PostgresEtlHook:
    def read_polar_with_exec(self, sql, connection_id, params=None, *, as_dataframe=False):
        hook = PostgresHook(postgres_conn_id=connection_id)
        if not as_dataframe:
            return hook.get_records(sql=sql, parameters=params)
        with closing(hook.get_conn()) as connection:
            return pl.read_database(
                query=sql,
                connection=connection,
                execute_options={'parameters': params} if params is not None else None,
                infer_schema_length=None,
            )
