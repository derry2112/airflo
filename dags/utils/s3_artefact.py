# Transcribed from the photographs supplied in chat.
import base64
from datetime import datetime, timedelta, timezone
import hashlib
import io
import json
import logging
import os
import time
from typing import List, Generator
from dags.utils.hooks.SFTPHookOcbc import SFTPHookOcbc
import polars as pl
from smart_open import open as smart_open
from dags.utils.hooks.S3HookOcbc import S3HookOcbc
from utils.telemetry.tracing import tracer
from airflow.sdk.bases.hook import BaseHook
import boto3
from boto3.s3.transfer import S3Transfer, TransferConfig


class S3ArtefactHook:
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def stream_s3_to_ftp(self, s3_url: str, sftp_conn_id: str,
                         remote_path: str, chunk_size: int = 8192):
        """
        Stream file dari S3 langsung ke FTP tanpa memuat ke RAM atau Disk.

        Args:
            s3_url: URL file di S3.
            ftp_conn: Koneksi FTP yang sudah aktif.
            remote_path: Path tujuan di FTP.
            chunk_size: Ukuran chunk per baca (default 8KB).
        """
        self.logger.info(f'[S3-FTP STREAM] Starting stream from {s3_url} to FTP {remote_path}')
        # Baca S3 menggunakan smart_open (streaming)
        s3_hook = S3HookOcbc(aws_conn_id='s3-artefact')
        client = s3_hook.get_conn()
        sftp_hook = SFTPHookOcbc(ssh_conn_id=sftp_conn_id)
        with smart_open(s3_url, 'rb', transport_params={'client': client}) as s3_file, \
                sftp_hook.get_conn() as sftp_conn, \
                sftp_conn.open(remote_path, 'wb') as remote_file:
            # Kirim ke SFTP per chunk
            while chunk := s3_file.read(chunk_size):
                remote_file.write(chunk)
        self.logger.info(f'[S3-FTP STREAM] Completed stream to {remote_path}')

    def stream_sftp_to_s3(self, sftp_conn_id: str, sftp_file_path: str,
                          workflow_name: str, app_name: str, ext=None) -> str:
        """
        Stream file dari SFTP langsung ke S3 tanpa memuat ke RAM atau Disk.

        Args:
            ftp_conn: Koneksi FTP yang sudah aktif.
            remote_path: Path tujuan di FTP.
        """
        self.logger.info('[SFTP-S3 STREAM] Starting stream SFTP to S3')
        self.logger.info(f'[SFTP-S3 STREAM] sftp_conn_id: {sftp_conn_id}')
        self.logger.info(f'[SFTP-S3 STREAM] sftp_file_path: {sftp_file_path}')
        s3_hook = S3HookOcbc(aws_conn_id='s3-artefact')
        client = s3_hook.get_conn()
        bucket_name = 'on-uat-airflow'
        if s3_hook.conn_extra:
            extra_json = json.loads(s3_hook.conn_extra)
            bucket_name = extra_json.get('bucket', bucket_name)
        sftp_hook = SFTPHookOcbc(ssh_conn_id=sftp_conn_id)
        today = datetime.now()
        date_file = today.strftime('%Y%m%d')
        s3_key = f'artefacts/{app_name}/{date_file}/{workflow_name}.{ext}'
        if ext is None:
            s3_key = f'artefacts/{app_name}/{date_file}/{workflow_name}'
        s3_url = f's3://{bucket_name}/{s3_key}'
        self.logger.info(f'[SFTP-S3 STREAM] s3 url: {s3_url}')
        with sftp_hook.get_conn() as sftp_conn, \
                sftp_conn.open(sftp_file_path, 'rb') as sftp_file:
            client.upload_fileobj(sftp_file, bucket_name, s3_key)
        self.logger.info('[SFTP-S3 STREAM] Completed stream SFTP to S3')
        return s3_url

    def put_file_to_s3(self, workflow_name=None, app_name=None, file_path=None,
                       ext='csv', date_file: datetime = None, is_periodic=False,
                       files: List[str] = None, is_delete_immediately: bool = True,
                       chunk_size: int = 50):
        """
        Uploads files to Amazon S3.

        :param self: The instance of the class.
        :param workflow_name: Name of the workflow. Used for S3 folder/object prefix.
        :type workflow_name: str, required.
        :param app_name: Name of the application. Used for S3 folder structure.
        :type app_name: str, required.
        :param file_path: FOR UPLOADING ONE FILE ONLY. Local path to the file to upload. Required unless `files` is provided.
        :type file_path: str, optional
        :param ext: File extension (e.g., 'csv', 'parquet'). Used for naming in S3.
        :type ext: str, optional
        :param date_file: Date reference for the file (e.g., for partitioning).
        :type date_file: datetime, optional
        :param is_periodic: Flag indicating if the upload is part of a periodic job.
        :type is_periodic: bool, optional
        :param files: FOR UPLOADING MULTIPLE FILES. If provided, this takes precedence over `file_path`.
        :type files: List[str], optional

        :return: None (upload succeeds or raises exception).
        :rtype: None
        """
        if date_file is None:
            today = datetime.now()
            date_file = today.strftime('%Y%m%d')
        connection_id = 's3-artefact'
        s3_hook = S3HookOcbc(aws_conn_id=connection_id)
        conn = BaseHook.get_connection(connection_id)
        self.logger.info(f'Conn: {conn}')
        self.logger.info(f'Conn Extra: {conn.extra}')
        bucket_name = 'on-sit-airflow'
        s3_url = ''
        if conn.extra:
            extra_json = json.loads(conn.extra)
            bucket_name = extra_json['bucket'] if 'bucket' in extra_json else bucket_name
        self.logger.info(f'S3 Bucket Name: {bucket_name}')
        s3_key = f'artefacts/{app_name}/{date_file}/{workflow_name}.{ext}'
        if ext is None:
            s3_key = f'artefacts/{app_name}/{date_file}/{workflow_name}'
        if is_periodic == True:
            s3_key = f'artefacts/{app_name}/{date_file}/{workflow_name}_periodic.{ext}'
        self.logger.info(f'File Path put: {file_path}')
        if files is None:
            try:
                client = s3_hook.get_conn()
                boto3.set_stream_logger('botocore', level=logging.DEBUG)
                # Configure chunk size (e.g., 1MB chunks)
                # minimum chunk size is 5MB. Max is 5GB.
                chunk_size_in_mb = chunk_size * 1024 * 1024
                config = TransferConfig(
                    multipart_threshold=chunk_size_in_mb,
                    multipart_chunksize=chunk_size_in_mb,
                    use_threads=True,  # Parallel uploads for parts
                    max_concurrency=10,  # Number of threads to use
                )
                self.logger.info(f'[UPLOAD CONFIG] Threshold: {chunk_size_in_mb/1024/1024}MB, ChunkSize: {chunk_size_in_mb/1024/1024}MB')
                transfer = S3Transfer(client=client, config=config)
                # Upload the content to S3
                file_path_upload = f'{file_path}.{ext}' if ext and ext not in file_path else file_path
                self.logger.info(f'Check local file location: {file_path_upload}')
                transfer.upload_file(filename=file_path_upload, key=s3_key, bucket=bucket_name)
                s3_url = f's3://{bucket_name}/{s3_key}'
                self.logger.info(f'Uploaded {file_path} to {s3_url}')
                if is_delete_immediately:
                    os.remove(f'{file_path_upload}')
                    self.logger.info(f'File {file_path_upload} is removed from local OCP')
            except Exception as ex:
                self.logger.warning(ex)
        else:
            try:
                for file in files:
                    # Upload the content to S3
                    s3_hook.load_file(filename=file, key=s3_key, bucket_name=bucket_name, replace=True)
                    s3_url = f's3://{bucket_name}/{s3_key}'
                    self.logger.info(f'Uploaded {file} to {s3_url}')
                    if is_delete_immediately:
                        os.remove(file)
                        self.logger.info(f'File {file} is removed from local OCP')
            except Exception as ex:
                self.logger.warning(ex)
        return s3_url

    def consume_file_in_s3_to_dataframe(self, connection_id='s3-artefact',
                                        separator='|', s3_url='',
                                        max_rows_per_chunk: int = 50_000,
                                        infer_schema_length: int = 100) -> Generator:
        """Returns a generator of DataFrames instead of one large DataFrame to prevent OOM."""
        generator = self.read_large_csv_in_chunks(
            connection_id=connection_id,
            separator=separator,
            s3_url=s3_url,
            max_rows_per_chunk=max_rows_per_chunk,
            infer_schema_length=infer_schema_length,
        )
        # Yield from the generator directly
        yield from generator

    def read_large_csv_in_chunks(self, s3_url: str,
                                  connection_id: str = 's3-artefact',
                                  separator: str = '|',
                                  max_rows_per_chunk: int = 50_000,
                                  infer_schema_length: int = 100) -> Generator:
        """
        Membaca file CSV besar dari S3 secara streaming (KOMPATIBEL dengan Airflow 3.1.8).

        CATATAN KOMPATIBILITAS:
        - polars==1.38.1 TIDAK DUKUNG scan_csv langsung dari s3:// URL tanpa plugin AWS.
        - Menggunakan smart_open (sudah di requirements.txt) untuk streaming line-by-line.
        - Polars read_csv digunakan untuk parsing chunk (lebih cepat daripada manual split).

        Args:
            s3_url: URL file CSV besar di S3.
            connection_id: ID koneksi S3.
            separator: Separator CSV.
            max_rows_per_chunk: Maksimal baris per chunk (default: 50k).

        Yields:
            pl.DataFrame: DataFrame berisi max_rows_per_chunk baris.
        """
        self.logger.info(f'[S3 READER] Starting streaming read for {s3_url} with chunk size {max_rows_per_chunk:,} rows...')
        start_time = time.time()
        try:
            s3_hook = S3HookOcbc(aws_conn_id=connection_id)
            client = s3_hook.get_conn()
            with smart_open(s3_url, 'r', transport_params={'client': client}) as f:
                # Baca header
                header_line = f.readline()
                if not header_line:
                    raise ValueError('File CSV is empty')
                columns = [col.strip() for col in header_line.strip().split(separator)]
                # Buffer untuk chunk
                chunk_lines = []
                rows_in_chunk = 0
                chunk_count = 0
                for line in f:
                    chunk_lines.append(line)
                    rows_in_chunk += 1
                    if rows_in_chunk >= max_rows_per_chunk:
                        chunk_count += 1
                        # Parse chunk dengan Polars (lebih cepat daripada manual)
                        csv_data = io.StringIO(''.join(chunk_lines))
                        df = pl.read_csv(
                            csv_data,
                            separator=separator,
                            has_header=False,
                            new_columns=columns,
                            dtypes={col: pl.Utf8 for col in columns},  # Force String untuk kecepatan
                            low_memory=True,
                            truncate_ragged_lines=True,
                            null_values=['', 'NULL', 'null', 'None'],
                            quote_char='"',  # AMAN: Handle quoted strings dengan benar (misal: "John ""Jack"" Doe")
                            infer_schema_length=infer_schema_length,
                        )
                        yield df
                        self.logger.info(f'[S3 READER] Chunk {chunk_count}: Yielded {df.height:,} rows')
                        # Reset buffer
                        chunk_lines = []
                        rows_in_chunk = 0
                # Sisa baris terakhir
                if chunk_lines:
                    chunk_count += 1
                    csv_data = io.StringIO(''.join(chunk_lines))
                    df = pl.read_csv(
                        csv_data,
                        separator=separator,
                        has_header=False,
                        new_columns=columns,
                        dtypes={col: pl.Utf8 for col in columns},
                        low_memory=True,
                        truncate_ragged_lines=True,
                        null_values=['', 'NULL', 'null', 'None'],
                        quote_char='"',  # AMAN: Handle quoted strings dengan benar
                    )
                    yield df
                    self.logger.info(f'[S3 READER] Final Chunk {chunk_count}: Yielded {df.height:,} rows')
            elapsed = time.time() - start_time
            self.logger.info(f'[S3 READER] Streaming read completed in {elapsed:.3f}s. Total chunks: {chunk_count}')
        except Exception as e:
            self.logger.error(f'[S3 READER] Failed to read CSV: {e}')
            raise RuntimeError(f'[S3 READER] Failed to read S3 file: {e}')
        elapsed = time.time() - start_time
        self.logger.info(f'[S3 READER] Streaming read completed in {elapsed:.3f}s')

    def _inject_content_md5(self, request, **kwargs):
        """
        Event handler to inject Content-MD5 header into delete_objects requests.
        Boto3 usually calculates this, but some providers (ViPR) require it explicitly.
        """
        # Get the body (XML)
        body = request.body
        if body:
            # Calculate MD5
            md5_hash = hashlib.md5(body).digest()
            md5_b64 = base64.b64encode(md5_hash).decode('utf-8')
            # Inject header
            request.headers['Content-MD5'] = md5_b64
            self.logger.info(f'Injected Content-MD5: {md5_b64}')

    def housekeep(self):
        """Scans an S3 bucket and deletes files older than the specified retention period."""
        connection_id = 's3-artefact'
        s3_hook = S3HookOcbc(aws_conn_id=connection_id)
        conn = BaseHook.get_connection(connection_id)
        self.logger.info(f'Conn Extra: {conn.extra}')
        # Determine Bucket Name
        bucket_name = 'on-sit-airflow'
        retention_days = 7
        if conn.extra:
            try:
                extra_json = json.loads(conn.extra)
                bucket_name = extra_json.get('bucket', bucket_name)
                retention_days = extra_json.get('retention_days', retention_days)
            except json.JSONDecodeError:
                self.logger.warning('Could not parse connection extra JSON')
        # Get the boto3 client
        s3_client = s3_hook.get_conn()
        # CRITICAL: Register the hook to inject Content-MD5 for delete_objects
        # This ensures ViPR receives the header it demands.
        s3_client.meta.events.register('before-sign.s3.DeleteObjects', self._inject_content_md5)
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=retention_days)
        self.logger.info(f'Starting housekeeping for bucket: {bucket_name}')
        self.logger.info(f'Deleting files older than: {cutoff_date}')
        paginator = s3_client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=bucket_name)
        delete_batch = []
        total_deleted = 0
        total_objects = 0
        for page in page_iterator:
            if 'Contents' not in page:
                continue
            for obj in page['Contents']:
                key = obj['Key']
                last_modified = obj['LastModified']
                # Check if object is older than retention period
                if last_modified < cutoff_date:
                    total_objects += 1
                    # Ensure key is a string
                    if isinstance(key, str):
                        delete_batch.append({'Key': key})
                        self.logger.info(f'Key {key}')
                    # Delete in batches of 100 (Smaller batch to avoid payload issues)
                    if len(delete_batch) >= 100:
                        self._delete_batch_safe(s3_client, bucket_name, delete_batch)
                        total_deleted += len(delete_batch)
                        delete_batch = []
        # Handle any remaining items
        if delete_batch:
            self._delete_batch_safe(s3_client, bucket_name, delete_batch)
            total_deleted += len(delete_batch)
        self.logger.info(f'Housekeeping complete. Found {total_objects} old objects. Deleted {total_deleted}.')

    def _delete_batch_safe(self, s3_client, bucket_name, delete_batch):
        """Attempts batch delete. If it fails with Content-MD5, falls back to single deletes."""
        if not delete_batch:
            return
        try:
            # Try batch delete first
            response = s3_client.delete_objects(Bucket=bucket_name, Delete={'Objects': delete_batch})
            deleted_count = len(response.get('Deleted', []))
            errors = response.get('Errors', [])
            if errors:
                self.logger.warning(f'Partial delete failed. Errors: {errors}')
                # If there are errors, we might need to retry those specific keys later
            self.logger.info(f'Batch deleted {deleted_count} objects.')
        except Exception as e:
            error_msg = str(e)
            if 'Content-MD5' in error_msg or 'InvalidRequest' in error_msg:
                self.logger.warning(f'Batch delete failed ({error_msg}). Falling back to single deletes for {len(delete_batch)} objects.')
                self._delete_single_objects_concurrent(s3_client, bucket_name, delete_batch)
            else:
                self.logger.error(f'Batch delete failed unexpectedly: {e}')
                raise e

    def _delete_single_objects_concurrent(self, s3_client, bucket_name, delete_batch):
        """
        Deletes objects one by one.
        For better performance, we could use ThreadPoolExecutor, but for simplicity
        and to avoid overloading the S3 endpoint, we loop here.
        """
        for item in delete_batch:
            key = item['Key']
            try:
                s3_client.delete_object(Bucket=bucket_name, Key=key)
            except Exception as e:
                self.logger.error(f'Failed to delete single object {key}: {e}')

    def count_rows_s3(self, s3_url: str, aws_conn_id: str = 's3-artefact') -> int:
        """
        Counts rows in an S3 file without loading it into RAM.

        Args:
            s3_url: The S3 URL (e.g., s3://bucket/key.csv)
            aws_conn_id: Airflow connection ID for S3

        Returns:
            int: Number of rows (excluding header if it's a CSV)
        """
        s3_hook = S3HookOcbc(aws_conn_id=aws_conn_id)
        client = s3_hook.get_conn()
        row_count = 0
        # smart_open streams the file, so it never loads the whole thing into RAM
        with smart_open(s3_url, 'r', transport_params={'client': client}) as f:
            # Skip header if it's a CSV (adjust based on your needs)
            if s3_url.lower().endswith('.csv'):
                header = next(f, None)
            for line in f:
                # Optional: Skip empty lines
                if line.strip():
                    row_count += 1
        self.logger.info(f'Data count from S3: {row_count}')
        return row_count

    def read_csv_fixed_width_to_dict(self, s3_url: str, header_schema: dict,
                                     details_schema: dict, trailer_schema: dict) -> dict:
        """
        Read fixed-width CSV file using Polars

        Args:
            s3_url: S3 URL (s3://bucket/key)
            header_schema: Schema for header records
            detail_schema: Schema for detail records
            trailer_schema: Schema for trailer records

        Returns:
            Polars DataFrame
        """
        try:
            s3_hook = S3HookOcbc(aws_conn_id='s3-artefact')
            client = s3_hook.get_conn()
            self.logger.info(f'[READ_S3_FIXED_WIDTH] Reading from: {s3_url}')
            df = None
            with smart_open(s3_url, 'r', transport_params={'client': client}) as f:
                # Baca lines
                lines = f.readlines()
                if not lines:
                    raise ValueError('File CSV is empty')
                self.logger.info(f'[READ_S3_FIXED_WIDTH] Loaded {len(lines)} lines')
                df = pl.DataFrame({'_raw': lines})
                df = df.with_columns(pl.col('_raw').str.slice(0, 1).alias('rec_id'))
                result = {'header': None, 'details': [], 'trailer': None}
                # Parse HEADER
                df_header = df.filter(pl.col('rec_id') == '1')
                if header_schema:
                    for col_name, (start, length) in header_schema.items():
                        df_header = df_header.with_columns(
                            pl.col('_raw').str.slice(start - 1, length).str.strip_chars().alias(col_name)
                        )
                result['header'] = df_header
                # Parse DETAIL
                df_details = df.filter(pl.col('rec_id') == '2')
                if details_schema:
                    for col_name, (start, length) in details_schema.items():
                        df_details = df_details.with_columns(
                            pl.col('_raw').str.slice(start - 1, length).str.strip_chars().alias(col_name)
                        )
                result['details'] = df_details
                # Parse TRAILER
                df_trailer = df.filter(pl.col('rec_id') == '9')
                if trailer_schema:
                    for col_name, (start, length) in trailer_schema.items():
                        df_trailer = df_trailer.with_columns(
                            pl.col('_raw').str.slice(start - 1, length).str.strip_chars().alias(col_name)
                        )
                result['trailer'] = df_trailer
                # Add row numbers
                if result['header'] is not None:
                    result['header'] = result['header'].with_columns(pl.int_range(0, pl.len()).alias('row_number'))
                if result['details'] is not None:
                    result['details'] = result['details'].with_columns(pl.int_range(0, pl.len()).alias('row_number'))
                if result['trailer'] is not None:
                    result['trailer'] = result['trailer'].with_columns(pl.int_range(0, pl.len()).alias('row_number'))
                self.logger.info('[READ_S3_FIXED_WIDTH] Parsed:')
                self.logger.info(f"  - Header: {result['header'].height if result['header'] is not None else 0} records")
                self.logger.info(f"  - Details: {result['details'].height if result['details'] is not None else 0} records")
                self.logger.info(f"  - Trailer: {result['trailer'].height if result['trailer'] is not None else 0} records")
            return result
        except Exception as e:
            self.logger.error(f'[READ_S3_FIXED_WIDTH] Error: {e}')
            raise
