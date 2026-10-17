import logging
import time
from typing import Generator

from dags.utils.hooks.SFTPHookOcbc import SFTPHookOcbc
from dags.utils.str_utils import StrUtils
import polars as pl
import io
from openpyxl import load_workbook
from collections import deque

class SftpHook:
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def put(self, file_path:str="", destination_path:str="", sftp_conn_id:str=""):
        """
        Docstring for putting file to destination

        :param file_path: Path of local file in OCP
        :type file_path: str
        :param destination_path: Destination Path in FTP.
        :type destination_path: str
        :param sftp_conn_id: FTP Connection ID
        :type sftp_conn_id: str
        """
        sftp_hook = SFTPHookOcbc(ssh_conn_id=sftp_conn_id)

        with sftp_hook.get_conn() as sftp_conn:
            sftp_conn.put(file_path, destination_path)

    def get(self, file_path:str="", destination_path:str="", sftp_conn_id:str=""):
        """
        Docstring for download file to airflow Persistent Volume

        :param file_path: File path in remote
        :type file_path: str
        :param destination_path: Destination Path in persistent volume.
        :type destination_path: str
        :param sftp_conn_id: SFTP Connection ID
        :type sftp_conn_id: str
        """
        sftp_hook = SFTPHookOcbc(ssh_conn_id=sftp_conn_id)

        with sftp_hook.get_conn() as sftp_conn:
            sftp_conn.get(file_path, destination_path)

    def copy(self, source_path: str, destination_path: str, sftp_conn_id: str):
        """
        Copy a file to another location on the same SFTP server
        (remote to remote over a single connection).

        Args:
            source_path: Source file path in SFTP.
            destination_path: Destination file path in SFTP.
            sftp_conn_id: SFTP Connection ID
        """
        str_utils = StrUtils()
        if str_utils.isEmpty(source_path) or str_utils.isEmpty(destination_path) or str_utils.isEmpty(sftp_conn_id):
            raise ValueError("Source Path, Destination Path and SFTP Connection ID must be filled")

        self.logger.info(f"[LOGGING] - [SFTP COPY] Copying {source_path} to {destination_path}")

        sftp_hook = SFTPHookOcbc(ssh_conn_id=sftp_conn_id)

        with sftp_hook.get_conn() as sftp_conn, \
                sftp_conn.open(source_path, "rb") as src_file, \
                sftp_conn.open(destination_path, "wb") as dst_file:
            while True:
                chunk = src_file.read(1024 * 1024)
                if not chunk:
                    break
                dst_file.write(chunk)

        self.logger.info(f"[LOGGING] - [SFTP COPY] Copy finished: {destination_path}")

    def cut(self, source_path: str, destination_path: str, sftp_conn_id: str):
        """
        Cut (move) a file to another location on the same SFTP server
        (remote to remote over a single connection, source removed after copy).

        Args:
            source_path: Source file path in SFTP.
            destination_path: Destination file path in SFTP.
            sftp_conn_id: SFTP Connection ID
        """
        str_utils = StrUtils()
        if str_utils.isEmpty(source_path) or str_utils.isEmpty(destination_path) or str_utils.isEmpty(sftp_conn_id):
            raise ValueError("Source Path, Destination Path and SFTP Connection ID must be filled")

        self.logger.info(f"[LOGGING] - [SFTP CUT] Cutting {source_path} to {destination_path}")

        sftp_hook = SFTPHookOcbc(ssh_conn_id=sftp_conn_id)

        with sftp_hook.get_conn() as sftp_conn:
            with sftp_conn.open(source_path, "rb") as src_file, \
                    sftp_conn.open(destination_path, "wb") as dst_file:
                while True:
                    chunk = src_file.read(1024 * 1024)
                    if not chunk:
                        break
                    dst_file.write(chunk)

            sftp_conn.remove(source_path)

        self.logger.info(f"[LOGGING] - [SFTP CUT] Cut finished: {source_path} moved to {destination_path}")

    def write_lines(self, lines: list, destination_path: str = "", sftp_conn_id: str = ""):
        """
        Write a list of strings directly to a remote file on SFTP, one string per line.
        Missing parent directories on SFTP are created automatically.

        Args:
            lines: List of strings to write, one per line
            destination_path: (Optional) Destination file path in SFTP.
            sftp_conn_id: (Optional) SFTP Connection ID
        """
        str_utils = StrUtils()
        if str_utils.isEmpty(destination_path) or str_utils.isEmpty(sftp_conn_id):
            raise ValueError("Destination Path and SFTP Connection ID must be filled")

        self.logger.info(f"[SFTP WRITE] Writing {len(lines)} lines to {destination_path}")

        sftp_hook = SFTPHookOcbc(ssh_conn_id=sftp_conn_id)

        with sftp_hook.get_conn() as sftp_conn:
            remote_file = sftp_conn.open(destination_path, "wb")
            for line in lines:
                remote_file.write((line + "\n").encode("utf-8"))
            remote_file.close()

        self.logger.info(f"[SFTP WRITE] Finished writing {len(lines)} lines to {destination_path}")

    def file_exists(self, sftp_path: str, sftp_conn_id: str) -> bool:
        """
        Check if a file exists on SFTP.

        Args:
            sftp_path: (Optional) Remote file path in SFTP.
            sftp_conn_id: (Optional) SFTP Connection ID

        Returns:
            bool: True if file exists, else False
        """
        str_utils = StrUtils()
        if str_utils.isEmpty(sftp_path) or str_utils.isEmpty(sftp_conn_id):
            raise ValueError("SFTP Path and SFTP Connection ID must be filled")

        sftp_hook = SFTPHookOcbc(ssh_conn_id=sftp_conn_id)

        with sftp_hook.get_conn() as sftp_conn:
            try:
                sftp_conn.stat(sftp_path)
                self.logger.info(f"[LOGGING] - [SFTP CHECK] File exists: {sftp_path}")
                return True
            except OSError:
                self.logger.info(f"[LOGGING] - [SFTP CHECK] File not found: {sftp_path}")
                return False

    def delete_file(self, sftp_path: str, sftp_conn_id: str):
        """
        Delete a file from SFTP.

        Args:
            sftp_path: Remote file path in SFTP.
            sftp_conn_id: SFTP Connection ID
        """
        str_utils = StrUtils()
        if str_utils.isEmpty(sftp_path) or str_utils.isEmpty(sftp_conn_id):
            raise ValueError("SFTP Path and SFTP Connection ID must be filled")

        if not self.file_exists(sftp_path=sftp_path, sftp_conn_id=sftp_conn_id):
            self.logger.info(f"[LOGGING] - [SFTP DELETE] File not found, nothing to delete: {sftp_path}")
            return

        sftp_hook = SFTPHookOcbc(ssh_conn_id=sftp_conn_id)

        with sftp_hook.get_conn() as sftp_conn:
            sftp_conn.remove(sftp_path)

        self.logger.info(f"[LOGGING] - [SFTP DELETE] File deleted: {sftp_path}")

    def read_fixed_width(self, remote_path: str, layout: list, sftp_conn_id: str, error_layout: list | None = None) -> tuple[pl.DataFrame, str]:
        """
        Read a fixed-width file directly from SFTP (streamed, no local temp file)
        and parse it into a Polars DataFrame.

        The file can match one of two record layouts: `layout` (validation
        succeeded, detail data) or `error_layout` (validation failed, 1 line).
        The matching layout is detected from the record length of the lines.

        Args:
            remote_path: File path in SFTP
            layout: Ordered list of (column_name, length) tuples.
            sftp_conn_id: SFTP Connection ID
            error_layout: (Optional) Ordered list of (column_name, length) tuples
                for the validation-failed file format.

        Returns:
            tuple: (pl.DataFrame, str) - Parsed DataFrame and detected record
                type ("success" or "error")
        """

        self.logger.info(f"[LOGGING] - [SFTP READ_FIXED_WIDTH] Reading {remote_path} directly from SFTP")

        sftp_hook = SFTPHookOcbc(ssh_conn_id=sftp_conn_id)

        with sftp_hook.get_conn() as sftp_conn:
            lines = []
            with sftp_conn.open(remote_path, "rb") as remote_file:
                for line_bytes in remote_file:
                    line_str = line_bytes.decode("utf-8", errors="replace").rstrip("\r\n")
                    if line_str.strip():
                        lines.append(line_str)

        self.logger.info(f"[LOGGING] - [SFTP READ_FIXED_WIDTH] All lines before parse ({len(lines)} lines):")
        self.logger.info("[LOGGING] - RAW\n" + "\n".join(lines))

        df, record_type = self.parse_fixed_width_lines(lines=lines, layout=layout, error_layout=error_layout)

        self.logger.info(f"[LOGGING] - [SFTP READ_FIXED_WIDTH] Parsed {df.height} rows ({record_type}) from {remote_path}")
        return df, record_type

    def read_fixed_width_to_dict(self, remote_path: str, header_schema: dict, details_schema: dict, trailer_schema: dict, sftp_conn_id: str) -> dict:
        """
        Read a fixed-width file directly from SFTP (streamed, no local temp file)
        and parse it into a dict of Polars DataFrames based on rec_id
        (1 = header, 2 = detail, 9 = trailer).

        Args:
            remote_path: File path in SFTP.
            header_schema: Schema for header records, {column: (start, length)} with 1-based start.
            details_schema: Schema for detail records, {column: (start, length)} with 1-based start.
            trailer_schema: Schema for trailer records, {column: (start, length)} with 1-based start.
            sftp_conn_id: SFTP Connection ID

        Returns:
            dict: {"header": pl.DataFrame, "details": pl.DataFrame, "trailer": pl.DataFrame}
        """

        self.logger.info(f"[LOGGING] - [SFTP READ_FIXED_WIDTH_DICT] Reading {remote_path} directly from SFTP")

        sftp_hook = SFTPHookOcbc(ssh_conn_id=sftp_conn_id)

        with sftp_hook.get_conn() as sftp_conn:
            lines = []
            with sftp_conn.open(remote_path, "rb") as remote_file:
                for line_bytes in remote_file:
                    line_str = line_bytes.decode("utf-8", errors="replace").rstrip("\r\n")
                    if line_str.strip():
                        lines.append(line_str)

        if not lines:
            raise ValueError(f"File {remote_path} is empty")

        self.logger.info(f"[LOGGING] - [SFTP READ_FIXED_WIDTH_DICT] All lines before parse ({len(lines)} lines):")
        self.logger.info("[LOGGING] - RAW\n" + "\n".join(lines))

        df = pl.DataFrame({"_raw": lines})
        df = df.with_columns(
            pl.col("_raw").str.slice(0, 1).alias("rec_id")
        )

        result = {
            "header": None,
            "details": None,
            "trailer": None
        }

        # Parse HEADER
        df_header = df.filter(pl.col("rec_id") == "1")
        if header_schema:
            for col_name, (start, length) in header_schema.items():
                df_header = df_header.with_columns(
                    pl.col("_raw").str.slice(start - 1, length).str.strip_chars().alias(col_name)
                )
        result["header"] = df_header

        # Parse DETAIL
        df_details = df.filter(pl.col("rec_id") == "2")
        if details_schema:
            for col_name, (start, length) in details_schema.items():
                df_details = df_details.with_columns(
                    pl.col("_raw").str.slice(start - 1, length).str.strip_chars().alias(col_name)
                )
        result["details"] = df_details

        # Parse TRAILER
        df_trailer = df.filter(pl.col("rec_id") == "9")
        if trailer_schema:
            for col_name, (start, length) in trailer_schema.items():
                df_trailer = df_trailer.with_columns(
                    pl.col("_raw").str.slice(start - 1, length).str.strip_chars().alias(col_name)
                )
        result["trailer"] = df_trailer

        # Add row numbers
        result["header"] = result["header"].with_columns(
            pl.int_range(0, pl.len()).alias("row_number")
        )
        result["details"] = result["details"].with_columns(
            pl.int_range(0, pl.len()).alias("row_number")
        )
        result["trailer"] = result["trailer"].with_columns(
            pl.int_range(0, pl.len()).alias("row_number")
        )

        self.logger.info(f"[LOGGING] - [SFTP READ_FIXED_WIDTH_DICT] Parsed:")
        self.logger.info(f"[LOGGING] - Header: {result['header'].height} records")
        self.logger.info(f"[LOGGING] - Details: {result['details'].height} records")
        self.logger.info(f"[LOGGING] - Trailer: {result['trailer'].height} records")

        return result

    def consume_file_in_sftp_to_dataframe(
            self, connection_id="",
            separator="|",
            sftp_path="",
            max_rows_per_chunk: int = 50_000,
            infer_schema_length: int = 100
    ) -> Generator:
        """
        Returns a generator of DataFrames instead of one large DataFrame to prevent OOM.
        """
        str_utils = StrUtils()
        if str_utils.isEmpty(connection_id):
            raise ValueError("Connection ID must be filled")

        generator = self.__read_large_csv_in_chunks__(
            connection_id=connection_id,
            separator=separator,
            sftp_path=sftp_path,
            max_rows_per_chunk=max_rows_per_chunk,
            infer_schema_length=infer_schema_length
        )
        # Yield from the generator directly
        for chunk_df in generator:
            self.logger.info(f"📦 [SFTP READER] Chunk Preview:")
            self.logger.info(chunk_df.head(5))
            yield chunk_df

    def __read_large_csv_in_chunks__(
        self,
        sftp_path: str,
        connection_id: str = "",
        separator: str = "|",
        max_rows_per_chunk: int = 50_000,  # Default 50k baris per chunk (Sangat Hemat RAM)
        infer_schema_length: int = 100
    ) -> Generator:
        """
        Membaca file CSV besar dari SFTP secara streaming (KOMPATIBEL dengan Airflow 3.1.8).

        CATATAN KOMPATIBILITAS:
        - Menggunakan smart_open (sudah di requirements.txt) untuk streaming line-by-line.
        - Polars read_csv digunakan untuk parsing chunk (lebih cepat daripada manual split).

        Args:
            sftp_path: URL file CSV besar di SFTP.
            connection_id: ID koneksi SFTP.
            separator: Separator CSV.
            max_rows_per_chunk: Maksimal baris per chunk (default: 50k).

        Yields:
            pl.DataFrame: DataFrame berisi max_rows_per_chunk baris.
        """
        str_utils = StrUtils()
        if str_utils.isEmpty(connection_id) :
            raise ValueError("Connection ID must be filled")

        self.logger.info(f"[SFTP READER] Starting streaming read for {sftp_path} with chunk size {max_rows_per_chunk:,} rows...")
        start_time = time.time()

        try:
            sftp_hook = SFTPHookOcbc(ssh_conn_id=connection_id)
            with sftp_hook.get_conn() as sftp_conn:
                file = sftp_conn.open(sftp_path, "rb")

                # Baca header
                header_line_bytes = file.readline()
                if not header_line_bytes:
                    raise ValueError("File CSV is empty")

                # Decode header, replacing invalid UTF-8 bytes
                header_line_str = header_line_bytes.decode('utf-8', errors='replace')

                # Remove BOM if present
                if header_line_str.startswith('\ufeff'):
                    header_line_str = header_line_str[1:]

                columns = [col.strip() for col in header_line_str.strip().split(separator)]

                # Buffer untuk chunk
                chunk_lines = []
                rows_in_chunk = 0
                chunk_count = 0

                for line_bytes in file:
                    line_str = line_bytes.decode('utf-8', errors='replace')
                    chunk_lines.append(line_str)
                    rows_in_chunk += 1

                    if rows_in_chunk >= max_rows_per_chunk:
                        chunk_count += 1
                        # Parse chunk dengan Polars (lebih cepat daripada manual)
                        csv_data = io.StringIO("".join(chunk_lines))
                        df = pl.read_csv(
                            csv_data,
                            separator=separator,
                            has_header=False,
                            new_columns=columns,
                            dtypes={col: pl.Utf8 for col in columns},  # Force String untuk kecepatan
                            low_memory=True,
                            truncate_ragged_lines=True,
                            null_values=["", "NULL", "null", "None"],
                            quote_char='"',  # AMAN: Handle quoted strings dengan benar (misal: "John "Jack" Doe")
                            infer_schema_length=infer_schema_length
                        )
                        yield df
                        self.logger.info(f"[SFTP READER] Chunk {chunk_count}: Yielded {df.height:,} rows")

                        # Reset buffer
                        chunk_lines = []
                        rows_in_chunk = 0

                # Sisa baris terakhir
                if chunk_lines:
                    chunk_count += 1
                    csv_data = io.StringIO("".join(chunk_lines))
                    df = pl.read_csv(
                        csv_data,
                        separator=separator,
                        has_header=False,
                        new_columns=columns,
                        dtypes={col: pl.Utf8 for col in columns},
                        low_memory=True,
                        truncate_ragged_lines=True,
                        null_values=["", "NULL", "null", "None"],
                        quote_char='"',  # AMAN: Handle quoted strings dengan benar
                    )
                    yield df
                    self.logger.info(f"[SFTP READER] Final Chunk {chunk_count}: Yielded {df.height:,} rows")

            elapsed = time.time() - start_time
            self.logger.info(f"[SFTP READER] Streaming read completed in {elapsed:.3f}s. Total chunks: {chunk_count}")
        except Exception as e:
            self.logger.error(f"[SFTP READER] Failed to read CSV: {e}")
            raise RuntimeError(f"[SFTP READER] Failed to read SFTP file: {e}")

        elapsed = time.time() - start_time
        self.logger.info(f"[SFTP READER] Streaming read completed in {elapsed:.3f}s")

    def count_rows_sftp(self, sftp_path: str, sftp_conn_id: str = "sftp_default") -> int:
        """
        Counts rows in an SFTP file without loading it into RAM.

        Args:
            sftp_path: The remote path to the file (e.g., "/data/file.csv").
            sftp_conn_id: Airflow connection ID for SFTP.

        Returns:
            int: Number of data rows (excluding header if it's a CSV).
        """
        str_utils = StrUtils()
        if str_utils.isEmpty(sftp_conn_id):
            raise ValueError("SFTP Connection ID must be filled")

        self.logger.info(f"[SFTP COUNT] Starting row count for {sftp_path}")
        row_count = 0
        has_header = sftp_path.lower().endswith('.csv')

        try:
            sftp_hook = SFTPHookOcbc(ssh_conn_id=sftp_conn_id)

            # Open in binary mode to handle any encoding safely
            with sftp_hook.get_conn() as sftp_conn:
                file = sftp_conn.open(sftp_path, "rb")

                # Skip header if it's a CSV
                if has_header:
                    header_line = file.readline()
                    if not header_line:
                        raise ValueError("File CSV is empty")
                    # We don't need to decode the header for counting.
                    # Just skip it. But if you want to log it:
                    # self.logger.info(f"Header skipped: {header_line.decode('utf-8', errors='replace').strip()}")

                # Count remaining lines
                for line in file:
                    # Skip empty lines (lines with only newline characters)
                    if line.strip():
                        row_count += 1

        except Exception as e:
            self.logger.error(f"[SFTP COUNT] Failed to count rows: {e}")
            raise RuntimeError(f"[SFTP COUNT] Failed to count SFTP file: {e}")

        self.logger.info(f"[SFTP COUNT] Total data rows found: {row_count}")
        return row_count

    def parse_fixed_width_lines(self, lines: list, layout: list, error_layout: list | None = None) -> tuple[pl.DataFrame, str]:
        """
        Parse a list of fixed-width lines into a Polars DataFrame.
        All columns are returned as string (whitespace stripped).

        The lines can match one of two record layouts: `layout` (validation
        succeeded, detail data) or `error_layout` (validation failed).
        The matching layout is detected from the record length of the lines.

        Args:
            lines: List of raw fixed-width lines (without newline characters)
            layout: Ordered list of (column_name, length) tuples
            error_layout: (Optional) Ordered list of (column_name, length) tuples
                for the validation-failed file format

        Returns:
            tuple: (pl.DataFrame, str) - Parsed DataFrame and detected record
                type ("success" or "error")
        """
        if not layout:
            raise ValueError("Layout must be filled")

        if not lines:
            raise ValueError("No lines to parse")

        record_length = sum(length for _, length in layout)
        record_type = "success"

        if error_layout:
            error_length = sum(length for _, length in error_layout)
            if error_length == record_length:
                raise ValueError("error_layout has the same record length as layout, cannot detect format by length")

            lengths_found = sorted({len(ln) for ln in lines})

            padded_error = all(len(ln.rstrip(" ")) <= error_length for ln in lines)

            if lengths_found == [record_length] and not padded_error:
                record_type = "success"
            elif lengths_found == [error_length] or padded_error:
                record_type = "error"
                layout = error_layout
                record_length = error_length
            else:
                raise ValueError(
                    f"Cannot detect record format: found line lengths {lengths_found}, "
                    f"expected {record_length} (success) or {error_length} (error)"
                )
            self.logger.info(f"[LOGGING] - [SFTP PARSE_FIXED_WIDTH] Detected record type: {record_type} ({record_length} chars/record)")
        else:
            bad_lines = [i for i, ln in enumerate(lines, start=1) if len(ln) != record_length]
            if bad_lines:
                raise ValueError(f"Record length != {record_length} at lines: {bad_lines[:5]}")

        df = pl.DataFrame({"_raw": lines})

        offset = 0
        for name, length in layout:
            df = df.with_columns(
                pl.col("_raw").str.slice(offset, length).str.strip_chars().alias(name)
            )
            offset += length

        df = df.drop("_raw")

        return df, record_type

    def consume_excel_in_sftp(
            self, connection_id="",
            sheet_name=None,
            sftp_path="",
            max_rows_per_chunk: int = 50_000,
            start_row: int = 1,
            start_column: int = 1,
            drop_last_rows: int=0
    ) -> Generator:
        """
        Returns a generator of DataFrames instead of one large DataFrame to prevent OOM.
        """
        str_utils = StrUtils()
        if str_utils.isEmpty(connection_id) :
            raise ValueError("Connection ID must be filled")

        generator = self.__read_excel_from_sftp__(
            connection_id=connection_id,
            sftp_path=sftp_path,
            sheet_name=sheet_name,
            max_rows_per_chunk=max_rows_per_chunk,
            start_column=start_column,
            start_row=start_row,
            drop_last_rows= drop_last_rows
        )
        # Yield from the generator directly
        for chunk_df in generator:
            self.logger.info(f"📦 [SFTP READER] Chunk Preview:")
            self.logger.info(chunk_df.head(5))
            yield chunk_df

    def __read_excel_from_sftp__(self, sftp_path: str,connection_id: str,sheet_name:str,max_rows_per_chunk:int=50_000,start_row: int = 1,start_column: int = 1,drop_last_rows: int=0):

        """
        Membaca file excel dari SFTP secara streaming

        Args:
            sftp_path: URL file excel di SFTP.
            connection_id: ID koneksi SFTP.
            max_rows_per_chunk: Maksimal baris per chunk (default: 50k).
            start_row: start row dari excel default 1
            start_column: start column dari excel default 1
            sheet_name: sheet yang ingin dibaca

        Yields:
            pl.DataFrame: DataFrame berisi max_rows_per_chunk baris.
        """
        str_utils = StrUtils()
        if str_utils.isEmpty(connection_id) :
            raise ValueError("Connection ID must be filled")

        self.logger.info(f"[SFTP READER] Starting streaming read for {sftp_path} with chunk size {max_rows_per_chunk:,} rows...")
        start_time = time.time()

        try:
            with SFTPHookOcbc(ssh_conn_id=connection_id).get_conn() as sftp_conn:
                file_bytes = io.BytesIO(sftp_conn.open(sftp_path, "rb").read())

            wb = load_workbook(file_bytes, read_only=True, data_only=True, keep_links=False)
            ws = wb[sheet_name] if sheet_name else wb.active
            self.logger.info(f"[SFTP READER] '{ws.title}': {ws.max_row}x{ws.max_column}")

            buffer = deque(maxlen=drop_last_rows)   # holds the trailing rows to discard
            chunk_rows, rows_in_chunk, chunk_count = [], 0, 0

            def flush(row):
                nonlocal chunk_rows, rows_in_chunk, chunk_count
                chunk_rows.append(row)
                rows_in_chunk += 1
                if rows_in_chunk >= max_rows_per_chunk:
                    chunk_count += 1
                    yielded = chunk_rows
                    chunk_rows, rows_in_chunk = [], 0
                    return yielded
                return None

            for row in ws.iter_rows(min_row=start_row, min_col=start_column, values_only=True):
                if drop_last_rows > 0:
                    if len(buffer) == buffer.maxlen:   # buffer full -> oldest is safe to emit
                        parked = buffer.popleft()
                        out = flush(parked)
                        if out is not None:
                            yield out
                    buffer.append(row)                # park the current row
                else:
                    out = flush(row)
                    if out is not None:
                        yield out

            # buffer now holds the last 'drop_last_rows' rows -> intentionally dropped
            if chunk_rows:
                chunk_count += 1
                yield chunk_rows

            wb.close()
            file_bytes.close()
        except Exception as e:
            self.logger.error(f"[SFTP READER] Failed to read CSV: {e}")
            raise RuntimeError(f"[SFTP READER] Failed to read SFTP file: {e}")

        elapsed = time.time() - start_time
        self.logger.info(f"[SFTP READER] Streaming read completed in {elapsed:.3f}s")
