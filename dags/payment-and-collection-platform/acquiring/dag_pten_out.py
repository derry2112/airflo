from datetime import datetime, timezone, timedelta
import logging
import os
from dags.utils.situtils.BaseDagsClient import ConfigClient
from airflow.sdk import dag, task, chain, Asset, Metadata
import time

from dags.utils.callback import Callback
from dags.utils.models.airflow_constant import RecordEstimate, StorageEstimate, TimeEstimate
from utils.telemetry.tracing import tracer
from dags.utils.sftp import SftpHook, SFTPHookOcbc
from typing import List, Set, NamedTuple
import polars as ps
from dags.utils.postgres import PostgresEtlHook
from dags.utils.oracle import OracleEtlHook
from dags.utils.operators.ssh.ssh_operator import SSHOperatorOcbc
import requests
import json


class ProcessDataMap(NamedTuple):
    pten_filename: str = ""
    mass_update_local_filepath: str = ""
    mass_update_remote_filepath: str = ""


logger = logging.getLogger(__name__)
sftp = SftpHook()
db_control = PostgresEtlHook()
db_source = OracleEtlHook()
config = {
    "owner": "acquiring",
    "pten_out_remote_folder_path": "/mtiuat/toocbc/PTEN/out",
    "db_control": "postgres-db-acquiring",
    "db_source": "oracle-db-sit-pwc",
    "sftp-pten": "sftp-pten",
    "ssh-pwc-bo": "ssh-pwc-bo",
    "sftp-pwc": "sftp-pwc",
    "mass_update_dest_folder_path": "/pwrcard/home/usr/data",
    "email_list": [],
}
FILE_PATH = os.path.dirname(os.path.realpath(__file__))
workflow_name = "dag_pten_out"
dag_name = workflow_name
callback = Callback(email_list=config["email_list"])
queue = "etl" if FILE_PATH.split("/")[-1] == "etl" else "default"
default_args = {
    "owner": config["owner"],
    "on_failure_callback": callback.task_failure_alert,
    "on_success_callback": callback.dag_success_alert,
    "queue": queue,
}
tz = timezone(timedelta(hours=7))
start_date = "2026-01-01"
timezone_used = "Asia/Jakarta"
user_cron = "15 * * * *"
endpoint = "/v1/qris-image-replace"
sit = {"basepath": "http://api-payment-sit.apps.np.ocbcnisp.com/"}
uat = {"basepath": "http://api-payment-uat.apps.np.ocbcnisp.com/"}
env = {"linkapi": sit["basepath"] + endpoint}


class ApiAcquiringFirstAcquirer:
    headers = {"accept": "application/json", "Content-Type": "application/json"}
    auth = ("test", "test")
    today = datetime.now().astimezone()
    linkapi = env.get("linkapi")
    param = {"filedate": today.strftime("%Y%m%d")}


app_name = FILE_PATH.split('/')[-4]
job_type = FILE_PATH.split('/')[-3]
job_operation = FILE_PATH.split('/')[-2]
dynamic_tags = {
    f"app:{app_name}",
    f"job:{job_type}",
    f"job:{job_type}:{job_operation}",
    "owner:acquiring",
    f"{TimeEstimate.BETWEEN_2M_5M}",
    f"{StorageEstimate.BELOW_1GB}",
    f"{RecordEstimate.BELOW_1K}",
}
dag_name = 'dag_pten_out'


@dag(
    dag_id=workflow_name,
    schedule=user_cron,
    description="",
    default_args=default_args,
    start_date=datetime.strptime(start_date, '%Y-%m-%d').astimezone(tz),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    tags=dynamic_tags,
    max_active_runs=1,
)
def dag_pten_out():
    @task
    @tracer.start_as_current_span(f"{dag_name}_task_get_unprocessed_file")
    def get_unprocessed_files() -> List[str]:
        processed_filenames = _get_processed_file()
        sftp_hook = SFTPHookOcbc(ssh_conn_id=config.get('sftp-pten'))
        conn = sftp_hook.get_conn()
        try:
            files = conn.listdir(config.get('pten_out_remote_folder_path'))
        finally:
            conn.close()
        logger.info("Files in %s: %s", config.get('pten_out_remote_folder_path'), files)

        # Filter: only files NOT yet in the control table
        # and excel only
        unprocessed = [
            f for f in files
            if f not in processed_filenames
            and f.endswith('.xlsx')
        ]
        if not unprocessed:
            logger.info("No unprocessed files found in %s", config.get('pten_out_remote_folder_path'))
            return []

        # matched = [f for f in files if pattern.search(f)]
        # logger.info("Matched files for %s: %s", today.date(), matched)
        # if not matched:
        #     logger.warning("No files found for today %s in %s", today.date(),
        #                    self.pten_out_remote_file_path)
        logger.info("Unprocessed files: %s", unprocessed)
        return unprocessed

    def _get_processed_file() -> Set[str]:
        db_control = PostgresEtlHook()
        q = "SELECT filename FROM pten_out_file_control p"
        processedFile = db_control.read_polars(sql=q, connection_id=config.get('db_control'))
        return set(processedFile["filename"].to_list())

    @task
    @tracer.start_as_current_span(f"{dag_name}_task_process_file")
    def process_file(files: List[str]) -> List[dict]:
        out_process_datamap = []
        today = datetime.now()
        base_headers = [
            "Nama Merchant (max 50)", "Nama Merchant (max 25)", "MPAN",
            "MID", "Kota", "Kodepos", "Kriteria", "MCC",
            "Jml Terminal", "Tipe Merchant", "NPWP", "KTP", "Tipe QR",
        ]
        raw_data_to_mass_update = ps.DataFrame(schema={"MID": ps.String, "NMID": ps.String, "MERCHANT_NUMBER": ps.String})
        for file in files:
            filepath = f"{config.get('pten_out_remote_folder_path')}/{file}"
            data = sftp.consume_excel_in_sftp(
                sftp_path=filepath,
                connection_id=config.get('sftp-pten'),
                start_row=5,
                drop_last_rows=1,
            )
            for chunk in data:
                try:
                    rawdf = ps.DataFrame(chunk, schema=ps.Schema(base_headers, [ps.String] * len(base_headers)))

                    if "NMID_QRIS_NMR" in file and "OCBC" in file:
                        cols = base_headers + ["NMID"]
                        rawdf.columns = cols
                        mids = rawdf["MID"].drop_nulls().unique().to_list()
                        merch_df = _lookup_merchant_number_pwc(db_source, mids)
                        merch_df = merch_df.rename({c: c.upper() for c in merch_df.columns})
                        df = rawdf.join(merch_df, on="MID", how="left")
                        df = df.with_columns(ps.col("MERCHANT_NUMBER").fill_null(""))
                        processed_df = df.select(["MID", "NMID", "MERCHANT_NUMBER"])
                        raw_data_to_mass_update = ps.concat(
                            [raw_data_to_mass_update, processed_df], ignore_index=True
                        )
                        for row in processed_df.iter_rows(named=True):
                            db_control.exec_cursor(
                                sql=(
                                    'UPDATE "QRISOnboardingLog_TH" '
                                    'SET status=%(status)s, NMID=%(nmid)s, updated_at=now() '
                                    'WHERE MID=%(mid)s'
                                ),
                                params={"mid": row["MID"], "nmid": row["NMID"], "status": "SUCCESS"},
                            )
                        mass_update_filepath = _write_batch_file(raw_data_to_mass_update, today)
                        _mark_file_processed(mass_update_filepath)
                        out_process_datamap.append(
                            ProcessDataMap(
                                mass_update_local_filepath=mass_update_filepath,
                                pten_filename=file,
                            )
                        )

                    elif ("FAILED_QRIS_NMR" in file or "FAILED_UPDATE_QRIS" in file) and "OCBC" in file:
                        cols = base_headers + ["STATUS"]
                        rawdf.columns = cols
                        processed_df = rawdf.select(["MID", "STATUS"])
                        for row in processed_df.iter_rows(named=True):
                            db_control.exec_cursor(
                                sql=(
                                    'UPDATE "QRISOnboardingLog_TH" '
                                    'SET status=%(status)s, error_message=%(message)s, updated_at=now() '
                                    'WHERE MID=%(mid)s'
                                ),
                                params={"mid": row["MID"], "message": row["STATUS"], "status": "FAILED"},
                                connection_id=config.get('db_control'),
                            )

                    elif "NMID_UPDATE_QRIS" in file and "OCBC" in file:
                        cols = base_headers + ["NMID"]
                        rawdf.columns = cols
                        mids = rawdf["MID"].drop_nulls().unique().to_list()
                        merch_df = _lookup_merchant_number_pwc(db_source, mids)
                        merch_df = merch_df.rename({c: c.upper() for c in merch_df.columns})
                        df = rawdf.join(merch_df, on="MID", how="left")
                        df = df.with_columns(ps.col("MERCHANT_NUMBER").fill_null(""))
                        processed_df = df.select(["MID", "NMID", "MERCHANT_NUMBER"])
                        raw_data_to_mass_update = ps.concat(
                            [raw_data_to_mass_update, processed_df], ignore_index=True
                        )
                        for row in processed_df.iter_rows(named=True):
                            db_control.exec_cursor(
                                sql=(
                                    'UPDATE "QRISOnboardingLog_TH" '
                                    'SET status=%(status)s, NMID=%(nmid)s, updated_at=now() '
                                    'WHERE MID=%(mid)s'
                                ),
                                params={"mid": row["MID"], "nmid": row["NMID"], "status": "SUCCESS"},
                                connection_id=config.get('db_control'),
                            )
                except (ValueError, TypeError) as e:
                    logger.warning(f"Skipping bad chunk: {e}")
                    continue
        return out_process_datamap

    def _lookup_merchant_number_pwc(db_source: OracleEtlHook, mids: List[str]):
        placeholders = ", ".join(f":mid_{j}" for j in range(len(mids)))
        params = {f"mid_{j}": m for j, m in enumerate(mids)}
        query = f"SELECT t.MER_ACCEPTOR_POINT_ID AS MID, t.MERCHANT_NUMBER AS MERCHANT_NUMBER FROM MER_ACCEPTOR_POINT t WHERE t.MER_ACCEPTOR_POINT_ID IN ({placeholders})"
        merch_rows = db_source.read_polar_with_exec(sql=query, connection_id=config.get('db_source'), params=params)
        return merch_rows

    def _write_batch_file(df: ps.DataFrame, today):
        if df.is_empty():
            logger.info("No records to write. Skipping.")
            return None
        df = df[["MID", "NMID", "MERCHANT_NUMBER"]].reset_index(drop=True)
        ts = today.strftime('%Y%m%d%H%M%S')
        filepath = f"{config.get('mass_update_dest_folder_path')}/MassUpdate_NMID_{ts}.txt"
        header = (
            f"HD"                         # RECORD_TYPE (2)       pos 1-2
            f"0000000000000000001"        # RECORD_SEQUENCE (19)  pos 3-21
            f"{'Mass Update':<20}"        # WORDING (20)          pos 22-41
            f"{today.strftime('%Y%m%d'):<8}"  # BUSINESS_DATE (8) pos 42-49
            f"{'1.5':<5}\n"             # VERSION (5)           pos 50-54
        )
        counter = 1
        record_count = 0  # default from header + footer
        contents = []
        contents.append(header)
        for row in df.iter_rows(index=True):
            logger.info("... MID=%r ...", row["MID"], row["MERCHANT_NUMBER"], row["NMID"])
            counter += 1
            record_count += 1
            content = (
                f"DT"
                f"{counter:0>19}"
                f"ID7339"
                f"{_pad_field(row['MID'], 15)}"
                f"{_pad_field(row['MERCHANT_NUMBER'], 15)}"
                f"{' ' * 40}"
                f"{' ' * 50}"
                f"{' ' * 3}"
                f"{' ' * 5}"
                f"{' ' * 64}"
                f"{' ' * 1}"
                f"{' ' * 3}"
                f"{_pad_field(row['NMID'], 80)}"
                f"{' ' * 80}"
                f"{' ' * 64}"
                f"{' ' * 64}"
                f"{' ' * 64}"
                f"{' ' * 64}"
                f"{' ' * 64}"
                f"{' ' * 3}"
                f"{' ' * 20}"
                f"{' ' * 10}"
                f"{' ' * 20}"
                f"{' ' * 50}"
                f"{' ' * 6}"
                f"{' ' * 6}"
                f"{' ' * 1}"
                f"{' ' * 1}"
                f"{' ' * 1}"
                f"{' ' * 1}"
                f"{' ' * 9}"
                f"{' ' * 1}"
                f"{' ' * 2}"
                f"{' ' * 4}"
                f"{' ' * 18}"
                f"{' ' * 4}"
                f"{' ' * 15}"
                f"{' ' * 2}"
                f"{' ' * 3}"
                f"{' ' * 8}"
                f"{' ' * 1}"
                f"{' ' * 32}"
                f"A"
                f"{' ' * 7}"
                f"{' ' * 50}"
                f"{' ' * 200}"
            )
            contents.append(content)
        footer = (
            f"TR"                         # RECORD_TYPE (2)
            f"0000000000000000001"        # RECORD_SEQUENCE (19)
            f"{record_count:0>19}"         # RECORDCOUNT
        )
        contents.append(footer)
        sftp.write_lines(lines=contents, destination_path=filepath, sftp_conn_id=config.get('sftp-pwc'))
        logger.info("Wrote %d records to %s", counter, filepath)
        return filepath

    def _pad_field(value, width: int) -> str:
        s = str(value) if value is not None and str(value) != 'nan' else ''
        return s[:width].ljust(width)

    @task
    @tracer.start_as_current_span(f"{dag_name}_task_check_status_mass_update")
    def check_status_mass_update():
        query_check = """
            SELECT
                MID,
                NEW_VALUE AS NMID
            FROM STG_PTEN_MERCHANT_UPDATES
            WHERE
                FIELD_NAME = :field
                AND (NEW_VALUE IS NOT NULL AND NEW_VALUE != '')
        """
        check_nmid = db_source.read_polar_with_exec(
            sql=query_check.strip(),
            params={"field": "NMID"},
            connection_id=config.get('db_source'),
        )
        if check_nmid.is_empty():
            logger.info("No mass update records found. Skipping.")
            return
        mids = check_nmid["MID"].to_list()
        placeholders = ", ".join([f"%(mid_{i})s" for i in range(len(mids))])
        params = {f"mid_{i}": m for i, m in enumerate(mids)}
        params["status"] = "SENT TO PWC"
        # kenapa SUCCESS? karena ini state status terakhir setelah airflow kirim file mass update ke pwc
        # status PENDING MASS UPDATE tidak terpakai
        params["toUpdateStatus"] = "SUCCESS"
        query_update_status = (
            'UPDATE "QRISOnboardingLog_TH" '
            'SET status = %(status)s, updated_at = now() '
            f'WHERE MID IN ({placeholders}) '
            'AND status = %(toUpdateStatus)s'
        )
        db_control.exec_cursor(sql=query_update_status, params=params, connection_id=config.get('db_control'))
        logger.info("Marked %d MIDs as SENT TO PWC.", len(mids))

    @task
    @tracer.start_as_current_span(f"{dag_name}_task_process_first_acquirer_file")
    def process_first_acquirer():
        query_check_first_acq = 'SELECT NMID FROM "QRISOnboardingLog_TH" WHERE STATUS = %(status)s'
        nmids = db_control.read_polar_with_exec(
            sql=query_check_first_acq,
            params={"status": "SENT TO PWC"},
            connection_id=config.get('db_control'),
        )
        if not nmids.is_empty():
            payload = {"nmids": nmids["NMID"].to_list()}
            response = requests.post(
                url=ApiAcquiringFirstAcquirer.linkapi,
                json=payload,
                headers=ApiAcquiringFirstAcquirer.headers,
                auth=ApiAcquiringFirstAcquirer.auth,
                timeout=30,
            )
            print('first acquirer operation triggered and response ', response)
        else:
            print('no nmid found for first acquirer, operation skipped')

    def _mark_file_processed(filename: str):
        q = (
            'INSERT INTO pten_out_file_control '
            '(filename, created_at) '
            'VALUES (%(filename)s, NOW())'
        )
        db_control.exec_cursor(sql=q, params={"filename": filename}, connection_id=config.get('db_control'))
        logger.info("Marked %s as processed", filename)

    @task
    @tracer.start_as_current_span(f"{dag_name}_task_process_remote_bash")
    def process_remote_bash(out_process_datamap: List):
        date_str = datetime.now().strftime('%d%m%Y')
        success_count = 0
        fail_count = 0
        for item in out_process_datamap:
            remote_filename = os.path.basename(item['mass_update_remote_filepath'])
            cmd = (
                f"/pwrcard/home/usr/shl/PWROCBC.sh mass_update_file_batch {date_str} "
                f"{remote_filename} "
                f"NULL NULL NULL NULL NULL NULL NULL NULL NULL"
            )
            logger.info("Running: %s", cmd)
            ssh_op = SSHOperatorOcbc(
                task_id='process-remote-bash',
                remote_host=config.get('ssh-pwc-bo'),
                command=str(cmd),
            )
            stdin, stdout, stderr = ssh_op.execute()
            output = stdout.read().decode()
            error = stderr.read().decode()
            exit_status = stdout.channel.recv_exit_status()
            if output:
                logger.info("[%s] stdout: %s", item.mass_update_remote_filepath, output.strip())
            if error:
                logger.warning("[%s] stderr: %s", item.mass_update_remote_filepath, error.strip())
            if exit_status == 0:
                success_count += 1
                _mark_file_processed(filename=item['pten_filename'])
            else:
                fail_count += 1
                logger.error("[%s] exit=%d, stderr=%s", item.mass_update_remote_filepath, exit_status, stderr)
            logger.info(
                "Processed %d file(s): %d succeeded, %d failed",
                len(out_process_datamap), success_count, fail_count,
            )

    check = check_status_mass_update()
    first_acq = process_first_acquirer()
    unprocessed = get_unprocessed_files()
    datamap = process_file(unprocessed)
    remote = process_remote_bash(datamap)
    chain(
        check,
        first_acq,
        unprocessed,
        datamap,
        remote,
    )


dag_pten_out()
