from __future__ import annotations

# Transcribed from WhatsApp Unknown 2026-10-26 at 22.35.17.zip.
# Completed with additional photographs supplied in chat on 2026-10-26.
import json
import logging
import os
import time
import types
from typing import Any, Dict, Generator, List, TYPE_CHECKING

from airflow.sdk import BaseHook
from sqlalchemy import Connection
if TYPE_CHECKING:
    from dags.utils.BaseDags.BaseDagsClient import ConfigClient
from dags.utils.conjur_secret_manager import ConjurSecretManager
from dags.utils.models.connection_model import ConnectionModel
from dags.utils.sftp import SftpHook
from dags.utils.str_utils import StrUtils
import polars as pl
from polars import DataFrame


class BaseSQL:
    """Base class containing all SQL templates and query logic."""

    def __init__(self):
        self.str_utils = StrUtils()
        self._s3_connection = None
        self.logger = logging.getLogger(__name__)
        self.conjur_manager = ConjurSecretManager()

    @property
    def s3_connection(self):
        if self._s3_connection is None:
            from dags.utils.s3_artefact import S3ArtefactHook
            self._s3_connection = S3ArtefactHook()
        return self._s3_connection

    def __setconnection__(self, connection_id: str = None) -> Connection:
        conn = BaseHook.get_connection(connection_id)
        extra_json = json.loads(conn.extra)
        connection_obj = ConnectionModel(
            username_dap=extra_json.get('username_dap', ''),
            password_dap=extra_json.get('password_dap', ''),
            host=conn.host,
            port=str(conn.port),
            schema=conn.schema,
        )
        self.logger.info(f'Username DAP: {connection_obj.username_dap}')
        self.logger.info(f'Password DAP: {connection_obj.password_dap}')
        conn_stream = Connection()
        return conn_stream

    def __setconnection_cursor__(self, connection_id: str = None) -> Connection:
        return BaseHook.get_connection(connection_id).get_hook().get_conn()

    def insert_rows(self, config: ConfigClient = None, s3_url: str = '',
                    connection_id: str = None, batch_size: int = 50000,
                    data_count: int = 0, separator: str = '|', max_retries: int = 3,
                    infer_schema_length: int = 100, source: str = 's3',
                    sftp_path: str = '', sftp_conn_id: str = None) -> bool:
        """Insert rows from CSV (S3/SFTP) into database with batching and retry logic.

        Reuses healthy connections, recreates broken connections, and commits
        each batch before moving to the next batch.
        """
        if not config:
            raise ValueError('config must be provided')
        s3_hook = self.s3_connection
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
                        chunk_df, config, placeholder_format='?'
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

    def _setup_columns_and_sql(self, chunk_df: DataFrame, config: ConfigClient,
                               placeholder_format: str = '?'):
        """Setup column mapping and generate INSERT SQL on first batch.

        Returns (columns, insert_sql, original_cols_to_select, rename_map).
        """
        raw_columns = chunk_df.columns
        if not raw_columns:
            raise ValueError('No columns found in source data')
        select_column = raw_columns
        original_cols_to_select = []
        rename_map = {}
        if config.select_column:
            # 1. Normalize target columns to lowercase
            target_cols_lower = [col.strip().lower() for col in config.select_column.lower().split(',')]
            # 2. Create column destination list
            if config.column_destination:
                select_column = [col.strip() for col in config.column_destination.split(',')]
            else:
                select_column = [col.strip() for col in config.select_column.split(',')]
            # 3. Create a map: lowercase_name -> original_csv_name
            csv_lookup = {col.lower(): col for col in raw_columns}
            for target in target_cols_lower:
                if target in csv_lookup:
                    original_name = csv_lookup[target]
                    original_cols_to_select.append(original_name)
                    rename_map[original_name] = target.lower()
                else:
                    raise ValueError(
                        f"Column '{target}' not found in source data. "
                        f'Available: {[c.lower() for c in raw_columns]}'
                    )
            # 4. Update columns to lowercase names for SQL
            columns = list(rename_map.values())
        else:
            columns = raw_columns
        if placeholder_format == 'oracle':
            placeholders = ', '.join([f':{i+1}' for i in range(len(columns))])
        elif placeholder_format == 'postgres':
            placeholders = ', '.join(['%s'] * len(columns))
        else:
            placeholders = ', '.join([placeholder_format] * len(columns))
        quoted_columns = ', '.join([f'"{col}"' for col in select_column])
        insert_sql = f'INSERT INTO {config.destination_schema}.{config.destination_table} ({quoted_columns}) VALUES ({placeholders})'
        self.logger.info(f'Generated INSERT SQL: {insert_sql}')
        return columns, insert_sql, original_cols_to_select, rename_map

    def _prepare_batch(self, chunk_df: DataFrame, original_cols_to_select: list, rename_map: dict):
        """Select original columns, rename them, and convert to list of tuples."""
        if original_cols_to_select:
            chunk_df = chunk_df.select(original_cols_to_select).rename(rename_map)
        batch = chunk_df.rows(named=False)
        return batch

    def _ensure_valid_connection(self, conn_stream, cursor, connection_id):
        """Return a valid (connection, cursor), reusing a healthy connection."""
        if conn_stream is None:
            conn_stream = self.__setconnection_cursor__(connection_id=connection_id)
            cursor = conn_stream.cursor()
            self.logger.info('✅ Created new connection and cursor')
            return conn_stream, cursor
        try:
            cursor.execute('SELECT 1')
            conn_stream.commit()
            return conn_stream, cursor
        except Exception as e:
            self.logger.warning(f'⚠ Connection broken: {e}. Recreating...')
            self._close_connection(conn_stream, cursor)
            conn_stream = self.__setconnection_cursor__(connection_id=connection_id)
            cursor = conn_stream.cursor()
            self.logger.info('✅ Created fresh connection after failure')
            return conn_stream, cursor

    def _close_connection(self, conn_stream, cursor):
        """Safely close connection and cursor."""
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass
        if conn_stream:
            try:
                conn_stream.close()
            except Exception:
                pass

    def mssql_insert_rows(self, config: ConfigClient = None, s3_url: str = '',
                          connection_id: str = None, batch_size=5000, data_count=0,
                          separator: str = '|', max_retries=3, infer_schema_length=0,
                          fast_executemany: bool = True) -> bool:
        import polars as pl
        import time
        if not config:
            raise ValueError('config must be provided')
        # CONNECTION
        conn = self.__setconnection_cursor__(connection_id=connection_id)
        cursor = conn.cursor()
        # cursor.fast_executemany = True  # performance boost
        cursor.fast_executemany = fast_executemany
        # STEP 1: READ DATA
        df = self.read_file_to_dataframe(separator=separator, s3_url=s3_url,
                                         infer_schema_length=infer_schema_length)
        self.logger.info(f'df type: {type(df)}')
        if isinstance(df, types.GeneratorType):
            self.logger.info('Generator detected, converting to DataFrame')
            chunks = list(df)
            # df = pl.concat(list(df))
            if not chunks:
                raise ValueError('No data returned from source file')
            df = pl.concat(chunks)
        self.logger.info(f'Loaded {df.height} rows')
        # STEP 2: CLEAN NULL
        df = df.with_columns([
            pl.when(pl.col(c).cast(str).str.strip_chars().is_in(['NULL', 'null', 'None', '', ' ', '__KFLOW_NULL__']))
            .then(None).otherwise(pl.col(c)).alias(c)
            for c in df.columns
        ])
        # STEP 3: GET SQL SCHEMA
        schema_sql = f"""
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{config.destination_schema}'
        AND TABLE_NAME = '{config.destination_table}'
        ORDER BY ORDINAL_POSITION
        """
        cursor.execute(schema_sql)
        schema_rows = cursor.fetchall()
        sql_schema = {row[0]: row[1] for row in schema_rows}
        self.logger.info(f'SQL schema: {sql_schema}')

        def map_sql_to_polars(dtype: str):
            dtype = dtype.lower()
            if dtype in ['varchar', 'nvarchar', 'char', 'text']:
                return pl.String
            elif dtype in ['int', 'bigint', 'smallint']:
                return pl.Int64
            elif dtype in ['decimal', 'numeric', 'float', 'real']:
                return pl.Float64
            elif dtype in ['bit']:
                return pl.Boolean
            elif dtype in ['datetime', 'smalldatetime', 'datetime2']:
                return pl.Datetime
            elif dtype in ['date']:
                return pl.Date
            else:
                return pl.String

        # STEP 5: CAST TYPES (ROBUST)
        for col, dtype in sql_schema.items():
            if col in df.columns:
                try:
                    polars_type = map_sql_to_polars(dtype)
                    # HANDLE DATETIME MULTI FORMAT
                    # if polars_type == pl.Datetime:
                    #     df = df.with_columns(
                    #         pl.coalesce([
                    #             pl.col(col).str.strptime(pl.Datetime, '%Y-%m-%d %H:%M:%S', strict=False),
                    #             pl.col(col).str.strptime(pl.Datetime, '%Y-%m-%d', strict=False),
                    #             pl.col(col).str.strptime(pl.Datetime, '%d/%m/%Y %H:%M:%S', strict=False),
                    #             pl.col(col).str.strptime(pl.Datetime, '%d/%m/%Y', strict=False),
                    #             pl.col(col).str.strptime(pl.Datetime, '%m/%d/%Y %H:%M:%S', strict=False),
                    #             pl.col(col).str.strptime(pl.Datetime, '%m/%d/%Y', strict=False),
                    #         ]).alias(col)
                    #     )
                    if polars_type == pl.Datetime:
                        col_str = pl.col(col).cast(str)
                        df = df.with_columns(
                            pl.coalesce([
                                col_str.str.strptime(pl.Datetime, '%Y-%m-%d %H:%M:%S%.f', strict=False),
                                col_str.str.strptime(pl.Datetime, '%Y-%m-%d %H:%M:%S', strict=False),
                                col_str.str.strptime(pl.Datetime, '%Y-%m-%dT%H:%M:%S%.f', strict=False),
                                col_str.str.strptime(pl.Datetime, '%Y-%m-%dT%H:%M:%S', strict=False),
                                col_str.str.strptime(pl.Datetime, '%Y-%m-%d', strict=False),
                                col_str.str.strptime(pl.Datetime, '%d/%m/%Y %H:%M:%S%.f', strict=False),
                                col_str.str.strptime(pl.Datetime, '%d/%m/%Y %H:%M:%S', strict=False),
                                col_str.str.strptime(pl.Datetime, '%d/%m/%Y', strict=False),
                                col_str.str.strptime(pl.Datetime, '%m/%d/%Y %H:%M:%S%.f', strict=False),
                                col_str.str.strptime(pl.Datetime, '%m/%d/%Y %H:%M:%S', strict=False),
                                col_str.str.strptime(pl.Datetime, '%m/%d/%Y', strict=False),
                            ]).alias(col)
                        )
                    elif polars_type == pl.Date:
                        col_str = pl.col(col).cast(str)
                        df = df.with_columns(
                            pl.coalesce([
                                col_str.str.strptime(pl.Date, '%Y-%m-%d', strict=False),
                                col_str.str.strptime(pl.Date, '%d/%m/%Y', strict=False),
                                col_str.str.strptime(pl.Date, '%m/%d/%Y', strict=False),
                            ]).alias(col)
                        )
                    else:
                        df = df.with_columns(pl.col(col).cast(polars_type, strict=False).alias(col))
                except Exception as e:
                    self.logger.warning(f'Cast failed for column {col}: {e}')
        # STEP 5.5: CLEAN INVALID DATETIME
        for col, dtype in sql_schema.items():
            if dtype.lower() in ['datetime', 'smalldatetime', 'datetime2']:
                if col not in df.columns:
                    continue
                try:
                    if df[col].dtype != pl.Datetime:
                        continue
                    null_count = df[col].null_count()
                    if null_count > 0:
                        self.logger.info(f'Column {col} has {null_count} null values after datetime parse (unparseable strings converted to null)')
                except Exception as e:
                    self.logger.warning(f'Could not validate datetime column {col}: {e}')
        # STEP 6: DATETIME FOR DB
        # df = df.with_columns([
        #     pl.when(
        #         pl.col(c).is_not_null() &
        #         (pl.col(c).cast(str).str.len_chars() > 0)
        #     )
        #     .then(pl.col(c).dt.strftime('%Y-%m-%d %H:%M:%S.%3f'))
        #     .otherwise(None)
        #     if df[c].dtype == pl.Datetime else pl.col(c)
        #     for c in df.columns
        # ])

        # df = df.with_columns([
        #     pl.when(
        #         pl.col(c).is_not_null() &
        #         (pl.col(c).cast(str).str.len_chars() > 0)
        #     )
        #     .then(pl.col(c).cast(str))  # KEEP ORIGINAL STRING
        #     .otherwise(None)
        #     if df[c].dtype == pl.Datetime else pl.col(c)
        #     for c in df.columns
        # ])
        df = df.with_columns([
            pl.when(pl.col(c).is_not_null()).then(pl.col(c)).otherwise(None)
            if df[c].dtype == pl.Datetime else pl.col(c)
            for c in df.columns
        ])
        # STEP 7: ALIGN COLUMN ORDER
        sql_columns = [row[0] for row in schema_rows]
        df = df.select([c for c in sql_columns if c in df.columns])
        # STEP 8: PREPARE INSERT
        columns = df.columns
        rows = df.rows()
        total_rows = len(rows)
        self.logger.info(f'Final row count: {total_rows}')
        if data_count and total_rows != data_count:
            raise ValueError(f'Expected {data_count}, got {total_rows}')
        if not columns:
            raise ValueError('No columns found in dataframe')
        placeholders = ', '.join(['?'] * len(columns))
        insert_sql = f"""
        INSERT INTO {config.destination_schema}.{config.destination_table}
        ({', '.join(columns)})
        VALUES ({placeholders})
        """
        self.logger.info(insert_sql)
        # STEP 9: INSERT BATCH
        inserted = 0
        for batch_idx in range(0, total_rows, batch_size):
            batch = rows[batch_idx:batch_idx + batch_size]
            retry = 0
            success = False
            while retry < max_retries and not success:
                try:
                    self.logger.info(f'Inserting batch {batch_idx}-{batch_idx + len(batch)} ({len(batch)} rows)')
                    cursor.executemany(insert_sql, batch)
                    conn.commit()
                    inserted += len(batch)
                    success = True
                except Exception as e:
                    retry += 1
                    self.logger.error(f'Retry {retry} failed: {e}')
                    time.sleep(30 * retry)
                    if retry >= max_retries:
                        conn.rollback()
                        raise ValueError(f'Insert failed after retries: {e}')
        if data_count and inserted != data_count:
            conn.rollback()
            raise ValueError(f'Mismatch insert: {inserted}/{data_count}')
        self.logger.info(f'Successfully inserted {inserted} rows into {config.destination_schema}.{config.destination_table}')
        cursor.close()
        conn.close()
        return True

    def insert_rows_burhan_to_sql(self, config: ConfigClient = None, s3_url: str = '',
                                  connection_id: str = None, batch_size=0, data_count=0,
                                  separator: str = '|', max_retries=3,
                                  batch_group_id: int = None,
                                  batch_group_column: str = 'BatchGroupId',
                                  column_hash: str = None, column_total_row: str = None,
                                  column_date: str = None, header_table: str = None,
                                  infer_schema_length=100):
        """Insert CSV S3 data for a burhan job, including batch_group_id."""
        if not config:
            raise ValueError('config must be provided')
        if not batch_group_id:
            raise ValueError('Batch group ID must be specified')
        conn_stream = self.__setconnection_cursor__(connection_id=connection_id)
        cursor = conn_stream.cursor()
        df = self.read_file_to_dataframe(separator=separator, s3_url=s3_url,
                                         infer_schema_length=infer_schema_length)
        columns: list[str] = []
        if self.str_utils.isEmpty(config.column_destination):
            columns = df.columns
        else:
            for sf in config.column_destination.split(','):
                sf = sf.strip()
                columns.append(sf)
        if batch_group_id is not None:
            df = df.with_columns(pl.lit(batch_group_id).alias(batch_group_column))
            columns.append(batch_group_column)
        self.logger.info(f'Columns: {columns}')
        rows = df.rows()
        total_rows = len(rows)
        self.logger.info(f'Total rows to insert: {total_rows}')
        if data_count and total_rows != data_count:
            raise ValueError(f'Expected {data_count} rows, but found {total_rows} in source.')
        if not columns:
            raise ValueError('No columns found in source data')
        if header_table == None or column_hash == None or column_total_row == None or column_date == None:
            raise ValueError(f"Header's data must be provided for inserting job to burhan. {header_table} | {column_hash} | {column_total_row} | {column_date}")
        placeholders = ', '.join(['?'] * len(columns))
        insert_sql = f"INSERT INTO {config.destination_schema}.{config.destination_table} ({', '.join(columns)}) VALUES ({placeholders})"
        self.logger.info(insert_sql)
        inserted = 0
        failed_batches: List[Dict] = []
        message = ''
        for batch_idx in range(0, total_rows, batch_size):
            batch = rows[batch_idx:batch_idx + batch_size]
            batch_start = batch_idx
            batch_end = min(batch_idx + batch_size, total_rows)
            batch_size_actual = len(batch)
            retry_count = 0
            success = False
            connection_failed = False
            while retry_count < max_retries and not success:
                try:
                    # Ensure connection is valid (only on first attempt or if reset).
                    if conn_stream is None or cursor is None:
                        conn_stream = self.__setconnection_cursor__(connection_id=connection_id)
                        cursor = conn_stream.cursor()
                        self.logger.info('✅ Created new connection and cursor.')
                    elif retry_count > 0:
                        # Close the old connection before retrying.
                        if cursor:
                            try:
                                cursor.close()
                                self.logger.info('Cursor closed for retry')
                            except:
                                pass
                        if conn_stream:
                            try:
                                conn_stream.close()
                                self.logger.info('Conn stream closed for retry')
                            except:
                                pass
                    if conn_stream is None or cursor is None:
                        conn_stream = self.__setconnection_cursor__(connection_id=connection_id)
                        cursor = conn_stream.cursor()
                        self.logger.info('✅ Created new connection and cursor.')
                    if connection_failed == True:
                        self.logger.warning('⚠ Connection test failed. Reconnecting...')
                        if cursor:
                            try:
                                cursor.close()
                            except:
                                pass
                        if conn_stream:
                            try:
                                conn_stream.close()
                            except:
                                pass
                        conn_stream = self.__setconnection_cursor__(connection_id=connection_id)
                        cursor = conn_stream.cursor()
                        cursor.execute('SELECT 1')
                        conn_stream.commit()
                        self.logger.info('✅ Fresh connection verified after test failure.')
                    # Attempt insert, trusting the existing connection.
                    self.logger.info(f'⏳ Batch {batch_idx//batch_size + 1} ({batch_start}-{batch_end}): Inserting {batch_size_actual} rows (retry {retry_count + 1}/{max_retries})')
                    cursor.executemany(insert_sql, batch)
                    conn_stream.commit()
                    inserted += batch_size_actual
                    success = True
                    self.logger.info(f'✅ Batch {batch_idx//batch_size + 1} committed successfully')
                except Exception as e:
                    retry_count += 1
                    self.logger.error(f'❌ Batch {batch_idx//batch_size + 1} failed (retry {retry_count}/{max_retries}): {str(e)}')
                    if cursor:
                        try:
                            cursor.close()
                            self.logger.info('Cursor closed after error')
                        except:
                            pass
                    if conn_stream:
                        try:
                            conn_stream.close()
                            self.logger.info('Conn stream closed after error')
                        except:
                            pass
                    connection_failed = True
                    if retry_count >= max_retries:
                        failed_batches.append({
                            'batch_index': batch_idx // batch_size,
                            'start_row': batch_start,
                            'end_row': batch_end,
                            'error': str(e),
                            'rows': batch_size_actual,
                        })
                        message = f'🔴 Batch {batch_idx//batch_size + 1} FAILED after {max_retries} retries.'
                        self.logger.error(message)
                        raise ValueError(message)
                    time.sleep(30 * retry_count)
                    self.logger.error(f'Sleep duration: {30*retry_count} seconds')
        if data_count and inserted != data_count:
            if conn_stream:
                conn_stream.rollback()
            raise ValueError(f'Row count mismatch: expected {data_count}, inserted {inserted}')
        if cursor:
            cursor.close()
        if conn_stream:
            conn_stream.close()
        return True

    def read_file_to_dataframe(self, separator: str = '|', s3_url: str = '',
                               infer_schema_length=100) -> DataFrame:
        df_generator = self.s3_connection.consume_file_in_s3_to_dataframe(
            connection_id='s3-artefact', separator=separator, s3_url=s3_url,
            infer_schema_length=infer_schema_length,
        )
        df_list: List[pl.DataFrame] = list(df_generator)
        if not df_list:
            return pl.DataFrame()
        combined_df = pl.concat(df_list, rechunk=True)
        return combined_df

    def read_polars(self, sql: str = None, connection_id: str = None):
        if not sql:
            raise ValueError('sql must be provided')
        try:
            conn_stream = self.__setconnection__(connection_id=connection_id)
            df = pl.read_database(query=sql, connection=conn_stream)
            conn_stream.close()
            return df
        except Exception as e:
            raise RuntimeError(str(e))

    def read_polars_in_chunks(self, sql=None, connection_id=None, batch_size=50000):
        """Read SQL in chunks to avoid OOM. Yield Polars DataFrames."""
        try:
            conn_stream = self.__setconnection__(connection_id=connection_id)
            self.logger.info(f'SQL Script: {sql}')
            offset = 0
            total_rows = 0
            while True:
                # TODO: might need to refactor query in the future.
                chunk_sql = f'{sql} ORDER BY 1 OFFSET {offset} ROWS FETCH NEXT {batch_size} ROWS ONLY'
                self.logger.info(f'Fetching chunk at offset {offset}')
                try:
                    df = pl.read_database(query=chunk_sql, connection=conn_stream)
                    if len(df) == 0:
                        self.logger.info('No more data. Finished chunking.')
                        break
                    yield df
                    total_rows += len(df)
                    offset += batch_size
                except Exception as e:
                    self.logger.error(f'Error fetching chunk at offset {offset}: {e}')
                    raise
            self.logger.info(f'Total rows processed: {total_rows}')
            conn_stream.close()
        except Exception as e:
            self.logger.error(f'Failed to connect or fetch: {e}')
            raise RuntimeError(str(e))

    def read_cursor(self, sql: str = None, connection_id: str = None):
        try:
            conn_stream = self.__setconnection_cursor__(connection_id=connection_id)
            cursor = conn_stream.cursor()
            cursor.execute(sql)
            rows = cursor.fetchall()
            cursor.close()
            conn_stream.close()
            return rows
        except Exception as e:
            raise RuntimeError(str(e))

    def exec_cursor(self, sql: str = None, connection_id: str = None, params=None):
        if not sql:
            raise ValueError('sql must be provided')
        try:
            conn_stream = self.__setconnection_cursor__(connection_id=connection_id)
            cursor = conn_stream.cursor()
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            conn_stream.commit()
            rows = cursor.rowcount
            cursor.close()
            conn_stream.close()
            self.logger.info(sql)
            result = f'{rows} record(s) affected'
            return result
        except Exception as e:
            raise RuntimeError(str(e))

    def create_backup_file_script(self, connection_id: str = None) -> str:
        self.logger.info('Template function')

    def read_polar_with_exec(self, sql: str = None, connection_id: str = None, params=None):
        """Execute SQL with native driver parameters and return a Polars DataFrame."""
        if not sql:
            raise ValueError('sql must be provided')
        conn_stream = None
        cursor = None
        try:
            conn_stream = self.__setconnection_cursor__(connection_id=connection_id)
            cursor = conn_stream.cursor()
            if params is not None:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            while cursor.description is None:
                nextset = getattr(cursor, 'nextset', None)
                if nextset is None or not nextset():
                    break
            if cursor.description is None:
                data = pl.DataFrame()
            else:
                columns = [col[0] for col in cursor.description]
                rows = [tuple(row) for row in cursor.fetchall()]
                data = pl.DataFrame(rows, schema=columns, orient='row', infer_schema_length=None)
            conn_stream.commit()
            return data
        except Exception as e:
            if conn_stream is not None:
                conn_stream.rollback()
            raise RuntimeError(str(e)) from e
        finally:
            self._close_connection(conn_stream, cursor)

    def write_csv_from_chunks(self, chunk_generator: Generator[DataFrame, Any, None] = None,
                              file_path='', separator='|', ext='csv') -> int:
        """Write a generator of DataFrame chunks into a single CSV."""
        if ext not in file_path:
            ext = ext.replace('.', '')
            file_path = f'{file_path}.{ext}'
        folder_path = os.path.dirname(file_path)
        os.makedirs(folder_path, exist_ok=True)
        first_chunk = True
        total_rows = 0
        for df in chunk_generator:
            with open(file_path, mode='a' if not first_chunk else 'w') as f:
                if first_chunk:
                    self.logger.info(df.head(10))
                    df.write_csv(f, include_header=True, separator='|')
                else:
                    df.write_csv(f, include_header=False, separator='|')
                self.logger.info(f"Mode: {'a' if not first_chunk else 'w'}")
            first_chunk = False
            total_rows += len(df)
            del df
        return total_rows
