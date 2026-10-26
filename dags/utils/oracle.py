"""Oracle query hook with optional Polars DataFrame results."""

from contextlib import closing

import polars as pl
from airflow.providers.oracle.hooks.oracle import OracleHook


class OracleEtlHook:
    def read_polar_with_exec(self, sql, connection_id, params=None, *, as_dataframe=False):
        hook = OracleHook(oracle_conn_id=connection_id)
        if not as_dataframe:
            return hook.get_records(sql=sql, parameters=params)
        with closing(hook.get_conn()) as connection:
            return pl.read_database(
                query=sql,
                connection=connection,
                execute_options={'parameters': params} if params is not None else None,
                infer_schema_length=None,
            )
