"""PostgreSQL connections, batched imports, and backup script generation."""

from contextlib import closing
import json
import shlex
import time
from urllib.parse import quote, urlencode

import polars as pl
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import BaseHook
from airflow.sdk.log import mask_secret
from sqlalchemy import Connection, PoolProxiedConnection, create_engine

from dags.utils.base_sql import BaseSQL
from dags.utils.models.connection_model import ConnectionModel
from dags.utils.s3_artefact import S3ArtefactHook
from dags.utils.sftp import SftpHook


class PostgresEtlHook(BaseSQL):
    def __init__(self, **kwargs):
        super().__init__()
        self.logger.info("CONNECT TO POSTGRES")

    def _create_engine(self, connection_id):
        conn = BaseHook.get_connection(connection_id)
        extra = json.loads(conn.extra or "{}")
        model = ConnectionModel(
            host=conn.host, port=conn.port or 5432, database=conn.schema,
            target_session_attrs=extra.get("target_session_attrs"),
            connect_timeout=extra.get("connect_timeout"),
        )
        username = (
            self.conjur_manager.get_conjur_value(extra["username_dap"])
            if extra.get("username_dap") else quote(conn.login or "", safe="")
        )
        password = (
            self.conjur_manager.get_conjur_value(extra["password_dap"])
            if extra.get("password_dap") else quote(conn.password or "", safe="")
        )
        query = {"host": model.host or "localhost",
                 "port": self.generate_multiple_port_for_db_ha(model)}
        for name in ("target_session_attrs", "connect_timeout"):
            value = getattr(model, name)
            if value is not None and value != "":
                query[name] = value
        url = (
            f"postgresql+psycopg2://{username}:{password}@/"
            f"{quote(model.database or '', safe='')}?{urlencode(query)}"
        )
        mask_secret(url)
        return create_engine(
            url, pool_size=1, max_overflow=0, pool_timeout=30,
            pool_pre_ping=True, pool_recycle=1800,
        )

    def __setconnection__(self, connection_id: str = None) -> Connection:
        try:
            return self._create_engine(connection_id).connect()
        except Exception as exc:
            raise RuntimeError("FAILED LOGIN") from exc

    def __setconnection_cursor__(self, connection_id: str = None) -> PoolProxiedConnection:
        try:
            return self._create_engine(connection_id).raw_connection()
        except Exception as exc:
            raise RuntimeError("FAILED LOGIN") from exc

    def insert_rows(self, config: object = None, s3_url: str = '',
                    connection_id: str = None, batch_size: int = 50000,
                    data_count: int = 0, separator: str = '|', max_retries: int = 3,
                    infer_schema_length: int = 100, source: str = 's3',
                    sftp_path: str = '', sftp_conn_id: str = None) -> bool:
        """Insert rows from CSV (S3/SFTP) into database with batching and retry logic.

        Reuses healthy connections, recreates broken connections, and commits
        each batch before moving to the next batch.
        """
        if source not in ('s3', 'sftp'):
            raise ValueError('source must be s3 or sftp')
        if batch_size < 1 or max_retries < 1:
            raise ValueError('batch_size and max_retries must be positive')
        if not config:
            raise ValueError('config must be provided')
        s3_hook = S3ArtefactHook()
        sftp_hook = SftpHook()
        rows_count = (
            s3_hook.count_rows_s3(s3_url=s3_url, aws_conn_id='s3-artefact')
            if source == 's3' else
            sftp_hook.count_rows_sftp(sftp_path=sftp_path, sftp_conn_id=sftp_conn_id)
        )
        if data_count != rows_count:
            raise ValueError(f'Expected {data_count} rows, but found {rows_count} in source.')
        chunk_size = min(batch_size, 50000)
        df_generator = (
            s3_hook.consume_file_in_s3_to_dataframe(
                connection_id='s3-artefact', separator=separator, s3_url=s3_url,
                max_rows_per_chunk=chunk_size, infer_schema_length=infer_schema_length,
            ) if source == 's3' else sftp_hook.consume_file_in_sftp_to_dataframe(
                connection_id=sftp_conn_id, separator=separator, sftp_path=sftp_path,
                max_rows_per_chunk=chunk_size, infer_schema_length=infer_schema_length,
            )
        )
        conn_stream = None
        cursor = None
        batch_idx = 0
        total_inserted = 0
        columns = None
        insert_sql = None
        original_cols_to_select = []
        rename_map = {}
        try:
            for chunk_df in df_generator:
                if columns is None:
                    columns, insert_sql, original_cols_to_select, rename_map = self._setup_columns_and_sql(
                        chunk_df, config, placeholder_format='postgres'
                    )
                batch = self._prepare_batch(chunk_df, original_cols_to_select, rename_map)
                if not batch:
                    continue
                batch_start = batch_idx
                batch_end = batch_idx + len(batch)
                for attempt in range(1, max_retries + 1):
                    try:
                        conn_stream, cursor = self._ensure_valid_connection(conn_stream, cursor, connection_id)
                        cursor.executemany(insert_sql, batch)
                        conn_stream.commit()
                        total_inserted += len(batch)
                        self.logger.info(
                            f'✅ Batch {batch_idx // batch_size + 1} ({batch_start}-{batch_end}): '
                            f'Inserted {len(batch)} rows (attempt {attempt}/{max_retries})'
                        )
                        break
                    except Exception as e:
                        self.logger.error(
                            f'❌ Batch {batch_idx // batch_size + 1} ({batch_start}-{batch_end}): '
                            f'Failed (attempt {attempt}/{max_retries}): {str(e)}'
                        )
                        if attempt == max_retries:
                            self._close_connection(conn_stream, cursor)
                            raise ValueError(f'Batch {batch_idx // batch_size + 1} failed after {max_retries} retries: {e}')
                        self._close_connection(conn_stream, cursor)
                        conn_stream = None
                        cursor = None
                        wait_time = 30 * attempt
                        self.logger.error(f'⏳ Waiting {wait_time} seconds before retry...')
                        time.sleep(wait_time)
                batch_idx += len(batch)
        except Exception as e:
            self.logger.error(f'Error in insert_rows: {e}')
            raise
        finally:
            self._close_connection(conn_stream, cursor)
        if data_count and total_inserted != data_count:
            raise ValueError(f'Row count mismatch: expected {data_count}, inserted {total_inserted}')
        self.logger.info(f'✅ Successfully inserted {total_inserted} rows')
        return True

    def create_backup_file_script(self, connection_obj: ConnectionModel = None) -> str:
        """Build a pg_dump script; credentials are plain text for libpq."""
        if connection_obj is None or not connection_obj.username_dap or not connection_obj.password_dap:
            raise ValueError("Username DAP and password DAP must be filled")
        username = self.conjur_manager.get_conjur_value_unparsed(connection_obj.username_dap)
        password = self.conjur_manager.get_conjur_value_unparsed(connection_obj.password_dap)
        host = connection_obj.host or "127.0.0.1"
        port = self.generate_multiple_port_for_db_ha(connection_obj) or "5432"
        database_arg = (
            "-d " + shlex.quote(connection_obj.database)
            if connection_obj.database else ""
        )
        script = f'''#!/bin/bash
set -e
export PGPASSWORD={shlex.quote(password)}
export PGHOST={shlex.quote(host)}
export PGPORT={shlex.quote(str(port))}
export PGUSER={shlex.quote(username)}
backup_dir=/home/airflow/backup_db
mkdir -p "$backup_dir"
DBS=$(psql {database_arg} -At -v ON_ERROR_STOP=1 -c "SELECT datname FROM pg_database WHERE datname NOT IN ('template0', 'template1', 'postgres')")
while IFS= read -r db; do
    [ -n "$db" ] || continue
    echo "Backing up $db..."
    pg_dump -d "$db" > "$backup_dir/$db.sql"
done <<< "$DBS"
sleep 2h
'''
        mask_secret(script)
        return script

    def generate_multiple_port_for_db_ha(self, connection_obj: ConnectionModel = None) -> str:
        if connection_obj is None:
            raise ValueError("connection_obj must be provided")
        hosts = [host.strip() for host in (connection_obj.host or "").split(",") if host.strip()]
        port = str(connection_obj.port or 5432)
        return ",".join([port] * max(len(hosts), 1))

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
