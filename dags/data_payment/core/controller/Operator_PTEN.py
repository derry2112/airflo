import pandas as pd
import os
import logging
from data_payment.core.connection.BaseConnection import DBConnection
from airflow.providers.ftp.hooks.ftp import FTPHook
from airflow.providers.sftp.hooks.sftp import SFTPHook
import xlsxwriter
from datetime import datetime
import re
import pathlib
from data_payment.api.api_acquiring_first_acquirer import ApiAcquiringFirstAcquirer
import time
import requests
from airflow.providers.ssh.hooks.ssh import SSHHook
from typing import NamedTuple

logger = logging.getLogger(__name__)


class Generate(object):
    def __init__(self, **kwargs):
        self.config = kwargs
        if self.config is None:
            print("Config is not defined!!!!")
        self.kwargs_db_data_source = self.config.get('kwargs_db_data_source')
        self.workflow_name = self.config.get('workflow_name')
        self.local_file_path = self.config.get('pten_in_file_path')
        self.remote_file_path = self.config.get('pten_in_remote_file_path')
        self.connectionftp = self.config.get('sftp_pten')
        self.kwargs_db_control_parameter_source = self.config.get('kwargs_db_control_parameter_source')

    def get_data_new(self, before, now):
        logging.info('START Get Data GET_DATA_NEW PTEN IN')
        db = DBConnection(**self.kwargs_db_data_source)
        new_merchant_query = """
        SELECT
            M.EXTERNAL_ID_1 AS NMID,
            M.ACRONYM AS MERCHANT_NAME,
            MA.AP_ON_ID_1 AS MPAN,
            MA.MER_ACCEPTOR_POINT_ID AS MID,
            C.CITY_NAME AS CITY,
            M.PR_ZIP_CODE AS POSTAL_CODE,
            M.STATUS_CODE AS STATUS,
            (SELECT CARACT_VALUE FROM MER_CONTRACT_CARACTERISTICS MRC WHERE MRC.CONTRACT_ELMT_ID = MA.CONTRACT_ELMT_ID AND CARACT_CONTEXT = 'MER_CRITERIA') AS MERCHANT_CRITERIA,
            MAC.MCC AS MCC,
            (SELECT CARACT_VALUE FROM MER_CONTRACT_CARACTERISTICS MRC WHERE MRC.CONTRACT_ELMT_ID = MA.CONTRACT_ELMT_ID AND CARACT_CONTEXT = 'TOTAL_STICKER') AS TOTAL_TERMINAL,
            CASE WHEN M.LEGAL_ID_TYPE = 'P' THEN '1' ELSE '2' END AS MERCHANT_TYPE,
            MA.LEGAL_ID AS NPWP,
            MR.LEGAL_ID AS KTP,
            (SELECT CARACT_VALUE FROM MER_CONTRACT_CARACTERISTICS MRC WHERE MRC.CONTRACT_ELMT_ID = MA.CONTRACT_ELMT_ID AND CARACT_CONTEXT = 'QRIS_TYPE') AS QR_TYPE,
            (SELECT CARACT_VALUE FROM MER_CONTRACT_CARACTERISTICS MRC WHERE MRC.CONTRACT_ELMT_ID = MA.CONTRACT_ELMT_ID AND CARACT_CONTEXT = 'MERCHANT_TYPE') AS MERCHANT_TYPE2,
            MA.OPENING_DATE AS OPENING_DATE
        FROM
            MER_ACCEPTOR_POINT MA,
            MERCHANT M,
            CITY C,
            MER_ACTIVITY MAC,
            MER_REPRESENTATIVE MR,
            MER_ACCEPTANCE_CONTRACT_ELMT mace
        WHERE
            MA.MERCHANT_NUMBER = M.MERCHANT_NUMBER
            AND (M.EXTERNAL_ID_1 IS NULL AND MA.PRIVATE_DATA_1 IS NULL)
            AND (M.PR_CITY_CODE = C.CITY_CODE AND M.PR_COUNTRY_CODE = C.COUNTRY_CODE)
            AND MA.ACTIVITY_ID = MAC.ACTIVITY_ID
            AND (MA.MERCHANT_NUMBER = MR.MERCHANT_NUMBER AND MR.REPRESENTATIVE_POSITION = '01')
            AND (MA.CONTRACT_ELMT_ID = mace.CONTRACT_ELMT_ID AND mace.PRODUCT_CODE = 'IDALL00T01')
            AND MA.OPENING_DATE > to_date(:lastrun, 'YYYY-MM-DD HH24:MI')
            AND MA.OPENING_DATE < to_date(:now, 'YYYY-MM-DD HH24:MI')
        """
        logging.info('Get Data New Merchant')
        records = db.execute_fetch_many_oracle(new_merchant_query, {'lastrun': before, 'now': now})
        logging.info('Finish Get Data New Merchant')
        return records

    def get_data_new_update(self, before, now):
        logging.info('START Get Data GET_DATA_NEW_UPDATE PTEN IN')
        db = DBConnection(**self.kwargs_db_data_source)
        new_merchant_query = """
        SELECT
            M.EXTERNAL_ID_1 AS NMID,
            M.ACRONYM AS MERCHANT_NAME,
            MA.AP_ON_ID_1 AS MPAN,
            MA.MER_ACCEPTOR_POINT_ID AS MID,
            C.CITY_NAME AS CITY,
            M.PR_ZIP_CODE AS POSTAL_CODE,
            M.STATUS_CODE AS STATUS,
            (SELECT CARACT_VALUE FROM MER_CONTRACT_CARACTERISTICS MRC WHERE MRC.CONTRACT_ELMT_ID = MA.CONTRACT_ELMT_ID AND CARACT_CONTEXT = 'MER_CRITERIA') AS MERCHANT_CRITERIA,
            MAC.MCC AS MCC,
            (SELECT CARACT_VALUE FROM MER_CONTRACT_CARACTERISTICS MRC WHERE MRC.CONTRACT_ELMT_ID = MA.CONTRACT_ELMT_ID AND CARACT_CONTEXT = 'TOTAL_STICKER') AS TOTAL_TERMINAL,
            CASE WHEN M.LEGAL_ID_TYPE = 'P' THEN '1' ELSE '2' END AS MERCHANT_TYPE,
            MA.LEGAL_ID AS NPWP,
            MR.LEGAL_ID AS KTP,
            (SELECT CARACT_VALUE FROM MER_CONTRACT_CARACTERISTICS MRC WHERE MRC.CONTRACT_ELMT_ID = MA.CONTRACT_ELMT_ID AND CARACT_CONTEXT = 'QRIS_TYPE') AS QR_TYPE,
            (SELECT CARACT_VALUE FROM MER_CONTRACT_CARACTERISTICS MRC WHERE MRC.CONTRACT_ELMT_ID = MA.CONTRACT_ELMT_ID AND CARACT_CONTEXT = 'MERCHANT_TYPE') AS MERCHANT_TYPE2,
            (SELECT CARACT_VALUE FROM MER_CONTRACT_CARACTERISTICS MRC WHERE MRC.CONTRACT_ELMT_ID = MA.CONTRACT_ELMT_ID AND CARACT_CONTEXT = 'MER_ACC_CHANNEL') AS ACCEPTANCE_CHANNEL
        FROM
            MER_ACCEPTOR_POINT MA,
            MERCHANT M,
            CITY C,
            MER_ACTIVITY MAC,
            MER_REPRESENTATIVE MR,
            STG_PTEN_MERCHANT_UPDATES SPU
        WHERE
            MA.MERCHANT_NUMBER = M.MERCHANT_NUMBER
            AND (M.EXTERNAL_ID_1 IS NULL AND MA.PRIVATE_DATA_1 IS NULL)
            AND (M.PR_CITY_CODE = C.CITY_CODE AND M.PR_COUNTRY_CODE = C.COUNTRY_CODE)
            AND MA.ACTIVITY_ID = MAC.ACTIVITY_ID
            AND (MA.MERCHANT_NUMBER = MR.MERCHANT_NUMBER AND MR.REPRESENTATIVE_POSITION = '01')
            AND SPU.MID = MA.MER_ACCEPTOR_POINT_ID
            AND (SPU.FLAG <> 'Y' OR SPU.FLAG IS NULL)
        """
        logging.info('Get Data New Update Merchant')
        records = db.execute_fetch_many_oracle(new_merchant_query, {'lastrun': before, 'now': now})
        logging.info('Finish Get Data New Update Merchant')
        return records

    def get_data_from_nmid(self):
        logging.info('START FETCH MERCHANT DETAILS BY MID')
        db = DBConnection(**self.kwargs_db_data_source)
        select_mid_query = """
            SELECT DISTINCT mid
            FROM STG_PTEN_MERCHANT_UPDATES
            WHERE FLAG = 'Y'
              AND FIELD_NAME LIKE '%STATUS%'
        """
        mids = db.execute_fetch_many_oracle(select_mid_query)
        mid_list = [row.mid for row in mids]
        if not mid_list:
            logging.info('MID not found.')
            return []
        merchant_detail_query = """
        SELECT
            M.EXTERNAL_ID_1 AS NMID,
            M.ACRONYM AS MERCHANT_NAME,
            MA.AP_ON_ID_1 AS MPAN,
            MA.MER_ACCEPTOR_POINT_ID AS MID,
            C.CITY_NAME AS CITY,
            M.PR_ZIP_CODE AS POSTAL_CODE,
            M.STATUS_CODE AS STATUS,
            (SELECT CARACT_VALUE FROM MER_CONTRACT_CARACTERISTICS MRC WHERE MRC.CONTRACT_ELMT_ID = MA.CONTRACT_ELMT_ID AND CARACT_CONTEXT = 'MER_CRITERIA') AS MERCHANT_CRITERIA,
            MAC.MCC AS MCC,
            (SELECT CARACT_VALUE FROM MER_CONTRACT_CARACTERISTICS MRC WHERE MRC.CONTRACT_ELMT_ID = MA.CONTRACT_ELMT_ID AND CARACT_CONTEXT = 'TOTAL_STICKER') AS TOTAL_TERMINAL,
            CASE WHEN M.LEGAL_ID_TYPE = 'P' THEN '1' ELSE '2' END AS MERCHANT_TYPE,
            MA.LEGAL_ID AS NPWP,
            MR.LEGAL_ID AS KTP,
            (SELECT CARACT_VALUE FROM MER_CONTRACT_CARACTERISTICS MRC WHERE MRC.CONTRACT_ELMT_ID = MA.CONTRACT_ELMT_ID AND CARACT_CONTEXT = 'QRIS_TYPE') AS QR_TYPE,
            (SELECT CARACT_VALUE FROM MER_CONTRACT_CARACTERISTICS MRC WHERE MRC.CONTRACT_ELMT_ID = MA.CONTRACT_ELMT_ID AND CARACT_CONTEXT = 'MER_ACC_CHANNEL') AS ACCEPTANCE_CHANNEL
        FROM
            MER_ACCEPTOR_POINT MA,
            MERCHANT M,
            CITY C,
            MER_ACTIVITY MAC,
            MER_REPRESENTATIVE MR
        WHERE
            TRUNC(MA.OPENING_DATE) = TRUNC(SYSDATE)
            AND (M.EXTERNAL_ID_1 IS NOT NULL AND MA.PRIVATE_DATA_1 IS NOT NULL)
            AND MA.MERCHANT_NUMBER = M.MERCHANT_NUMBER
            AND (M.PR_CITY_CODE = C.CITY_CODE AND M.PR_COUNTRY_CODE = C.COUNTRY_CODE)
            AND MA.ACTIVITY_ID = MAC.ACTIVITY_ID
            AND (MA.MERCHANT_NUMBER = MR.MERCHANT_NUMBER AND MR.REPRESENTATIVE_POSITION = '01')
            AND M.EXTERNAL_ID_1 = ANY(:mid_list)
        """
        merchant_rows = db.execute_fetch_many_oracle(merchant_detail_query, {'mid_list': mid_list})
        merchant_data = [dict(row._mapping) for row in merchant_rows]
        logging.info(f'Total merchant details fetched: {len(merchant_data)}')
        return merchant_data

    def get_data_update(self, before, now):
        logging.info('START Get Data GET_DATA_UPDATE PTEN IN')
        db = DBConnection(**self.kwargs_db_data_source)
        updated_merchant_query = """
        SELECT
            M.EXTERNAL_ID_1 AS NMID,
            M.ACRONYM AS MERCHANT_NAME,
            MA.AP_ON_ID_1 AS MPAN,
            MA.MER_ACCEPTOR_POINT_ID AS MID,
            C.CITY_NAME AS CITY,
            M.PR_ZIP_CODE AS POSTAL_CODE,
            M.STATUS_CODE AS STATUS,
            (SELECT CARACT_VALUE FROM MER_CONTRACT_CARACTERISTICS MRC WHERE MRC.CONTRACT_ELMT_ID = MA.CONTRACT_ELMT_ID AND CARACT_CONTEXT = 'MER_CRITERIA') AS MERCHANT_CRITERIA,
            MAC.MCC AS MCC,
            (SELECT CARACT_VALUE FROM MER_CONTRACT_CARACTERISTICS MRC WHERE MRC.CONTRACT_ELMT_ID = MA.CONTRACT_ELMT_ID AND CARACT_CONTEXT = 'TOTAL_STICKER') AS TOTAL_TERMINAL,
            CASE WHEN M.LEGAL_ID_TYPE = 'P' THEN '1' ELSE '2' END AS MERCHANT_TYPE,
            MA.LEGAL_ID AS NPWP,
            MR.LEGAL_ID AS KTP,
            (SELECT CARACT_VALUE FROM MER_CONTRACT_CARACTERISTICS MRC WHERE MRC.CONTRACT_ELMT_ID = MA.CONTRACT_ELMT_ID AND CARACT_CONTEXT = 'QRIS_TYPE') AS QR_TYPE,
            (SELECT CARACT_VALUE FROM MER_CONTRACT_CARACTERISTICS MRC WHERE MRC.CONTRACT_ELMT_ID = MA.CONTRACT_ELMT_ID AND CARACT_CONTEXT = 'MERCHANT_TYPE') AS MERCHANT_TYPE2,
            (SELECT CARACT_VALUE FROM MER_CONTRACT_CARACTERISTICS MRC WHERE MRC.CONTRACT_ELMT_ID = MA.CONTRACT_ELMT_ID AND CARACT_CONTEXT = 'MER_ACC_CHANNEL') AS ACCEPTANCE_CHANNEL
        FROM
            MER_ACCEPTOR_POINT MA,
            MERCHANT M,
            CITY C,
            MER_ACTIVITY MAC,
            MER_REPRESENTATIVE MR,
            STG_PTEN_MERCHANT_UPDATES SPU
        WHERE
            MA.MERCHANT_NUMBER = M.MERCHANT_NUMBER
            AND (M.EXTERNAL_ID_1 IS NOT NULL AND MA.PRIVATE_DATA_1 IS NOT NULL)
            AND (M.PR_CITY_CODE = C.CITY_CODE AND M.PR_COUNTRY_CODE = C.COUNTRY_CODE)
            AND MA.ACTIVITY_ID = MAC.ACTIVITY_ID
            AND (MA.MERCHANT_NUMBER = MR.MERCHANT_NUMBER AND MR.REPRESENTATIVE_POSITION = '01')
            AND SPU.MID = MA.MER_ACCEPTOR_POINT_ID
            AND SPU.FLAG <> 'Y'
            AND (SPU.FIELD_NAME <> 'EXTERNAL_ID_1' OR SPU.FIELD_NAME <> 'NMID')
        """
        logging.info('Get Data Update Merchant')
        records = db.execute_fetch_many_oracle(updated_merchant_query, {'lastrun': before, 'now': now})
        logging.info('Finish Get Data Update Merchant')
        return records

    def write_data(self, datas, counter, mode="NEW"):
        os.makedirs(self.local_file_path, exist_ok=True)
        today = datetime.now()
        strdate = today.strftime('%Y%m%d')
        col_header = [
            "Nama Merchant (max 50)", "Nama Merchant (max 25)", "MPAN", "MID",
            "Kota", "Kodepos", "Kriteria", "MCC", "Jml Terminal", "Tipe Merchant",
            "NPWP", "KTP", "Tipe QR", "Channel QR", "Tanggal Buka",
        ]
        df = pd.DataFrame(datas)
        if df.empty:
            logging.warning('write_data: empty DataFrame, skipping file write')
            return None, df
        logging.info(f'DEBUG mode={mode}: columns = {list(df.columns)}')
        df.insert(2, 'Nama Merchant (max 25)', df['MERCHANT_NAME'].astype(str).str.slice(0, 25))
        if mode == 'NEW':
            filename = f'QRIS_NMR_93600028_{strdate}_NEW_OCBC{counter:06}.xlsx'
            header1 = 'FORM PENDAFTARAN'
            header2 = 'NATIONAL MERCHANT REPOSITORY QRIS'
            col_header.extend(['Interface', 'Model Usaha'])
            df['interface'] = '1'
            df['model usaha'] = 'model_usaha'
            df = df.drop(columns=['NMID', 'STATUS'], errors='ignore')
        elif mode == 'UPDATE':
            filename = f'UPDATE_QRIS_NMR_93600028_{strdate}_NEW_OCBC{counter:06}.xlsx'
            header1 = 'FORM UPDATE'
            header2 = 'NATIONAL MERCHANT REPOSITORY QRIS'
            col_header.extend(['Interface', 'Model Usaha', 'Keterangan UPDATE'])
            df['interface'] = '1'
            df['model_usaha'] = 'model_usaha'
            df['update_reason'] = 'Data Merchant Update'
            df = df[[
                'merchant_name', 'Nama Merchant (max 25)', 'mpan', 'mid', 'city',
                'postal_code', 'merchant_criteria', 'mcc', 'total_terminal',
                'merchant_type', 'npwp', 'ktp', 'qr_type', 'channel_qr',
                'interface', 'model_usaha', 'nmid', 'update_reason',
            ]]
        elif mode == 'DELETE':
            filename = f'DELETE_QRIS_NMR_93600028_{strdate}_NEW_OCBC{counter:06}.xlsx'
            header1 = 'FORM PENGHAPUSAN'
            header2 = 'NATIONAL MERCHANT REPOSITORY QRIS'
        df.columns = col_header
        df.insert(0, 'No', range(1, len(df) + 1))
        full_path = os.path.join(self.local_file_path, filename)
        wb = xlsxwriter.Workbook(full_path)
        ws = wb.add_worksheet('Sheet1')
        ws.write(0, 0, header1)
        ws.write(1, 0, header2)
        for col_num, col_name in enumerate(df.columns):
            ws.write(2, col_num, col_name)
        for row_idx, row_data in enumerate(df.itertuples(index=False)):
            for col_num, value in enumerate(row_data):
                ws.write(3 + row_idx, col_num, value)
        ws.write(len(df) + 3, 0, 'end')
        wb.close()
        return full_path, df

    def main_process(self):
        db_control = DBConnection(**self.kwargs_db_control_parameter_source)
        today = datetime.now()
        db_source = DBConnection(**self.kwargs_db_data_source)
        data_changes = db_source.execute_fetch_many_oracle(
            'SELECT DISTINCT MID FROM STG_PTEN_MERCHANT_UPDATES WHERE FLAG<>:flag',
            {'flag': 'Y'},
        )
        check_data_changes = len(data_changes) != 0
        lastrun = db_control.execute_scalar(
            'select value from "PTENControlTable_TN" where "keys" = :keys',
            {'keys': 'last_run_date'},
        )
        date_last_run = datetime.fromisoformat(str(lastrun))
        if date_last_run.strftime('%Y-%m-%d') != today.strftime('%Y-%m-%d'):
            counter = 1
        else:
            counter = int(db_control.execute_scalar(
                'select value from "PTENControlTable_TN" where keys=:keys',
                {'keys': 'sequence'},
            ))
        data_new = pd.DataFrame(
            self.get_data_new(date_last_run, today.strftime('%Y-%m-%d %H:%M')),
            columns=[
                'NMID', 'MERCHANT_NAME', 'MPAN', 'MID', 'CITY', 'POSTAL_CODE', 'STATUS',
                'MERCHANT_CRITERIA', 'MCC', 'TOTAL_TERMINAL', 'MERCHANT_TYPE',
                'NPWP', 'KTP', 'QR_TYPE', 'MERCHANT_TYPE2', 'OPENING_DATE',
            ],
        )
        header_raw = [
            'NMID', 'MERCHANT_NAME', 'MPAN', 'MID', 'CITY', 'POSTAL_CODE', 'STATUS',
            'MERCHANT_CRITERIA', 'MCC', 'TOTAL_TERMINAL', 'MERCHANT_TYPE',
            'NPWP', 'KTP', 'QR_TYPE', 'MERCHANT_TYPE2', 'OPENING_DATE',
        ]
        try:
            logging.info('Insert data to Onboarding Logs')
            df_log_data = pd.DataFrame(data_new)
            df_log_data.columns = header_raw
            df_log_data = df_log_data[['MID', 'OPENING_DATE']]
            df_log_data['PERIOD'] = today.strftime('%Y%m%d')
            df_log_data = df_log_data[['MID', 'OPENING_DATE']]
            df_log_data['PERIOD'] = today.strftime('%Y%m%d')
            df_log_data['STATUS'] = 'PENDING'
            while True:
                logging_data = df_log_data[:5000]
                logging_data.columns = ['mid', 'opening_date', 'period', 'status']
                db_control.bulk_insert(
                    'QRISOnboardingLog_TH', ['mid', 'opening_date', 'period', 'status'],
                    logging_data.to_dict('records'),
                )
                logging.info('Successfully insert log data to table QRISOnboardingLog_TH')
                df_log_data = df_log_data[5000:]
                if len(df_log_data) < 1:
                    break
        except Exception as e:
            logging.error('Failed insert log data to table QRISOnboardingLog_TH: %s', e)

        while len(data_new) > 0:
            processed_data = data_new[:5000]
            filename, df_processed = self.write_data(processed_data, counter, mode='NEW')
            if filename is None:
                break
            mid = tuple(df_processed['MID'].to_list())
            db_control.execute_no_result(
                'UPDATE "QRISOnboardingLog_TH" SET filename = :filename WHERE mid IN :mid',
                {'filename': filename, 'mid': mid},
            )
            data_new = data_new[5000:]
            counter += 1

        if check_data_changes:
            columns = [
                'NMID', 'MERCHANT_NAME', 'MPAN', 'MID', 'CITY', 'POSTAL_CODE', 'STATUS',
                'MERCHANT_CRITERIA', 'MCC', 'TOTAL_TERMINAL', 'MERCHANT_TYPE',
                'NPWP', 'KTP', 'QR_TYPE', 'MERCHANT_TYPE2', 'ACCEPTANCE_CHANNEL',
            ]
            data_new_update = pd.DataFrame(
                self.get_data_new_update(date_last_run, today.strftime('%Y-%m-%d %H:%M')),
                columns=columns,
            )
            mid_updated = tuple(pd.DataFrame(data_new_update).select('MID')['MID'].to_list())
            db_control.execute_no_result(
                'UPDATE "QRISOnboardingLog_TH" SET status=:status, updated_at=now() WHERE mid IN :mid',
                {'status': 'PENDING', 'mid': mid_updated},
            )
            while len(data_new_update) > 0:
                processed_data = data_new_update[:5000]
                filename, df_processed = self.write_data(processed_data, counter, mode='NEW')
                if filename is None:
                    break
                mid = tuple(df_processed['MID'].to_list())
                db_control.execute_no_result(
                    'UPDATE "QRISOnboardingLog_TH" SET filename=:filename WHERE mid IN :mid',
                    {'filename': filename, 'mid': mid},
                )
                data_new_update = data_new_update[5000:]
                counter += 1

            data_update = pd.DataFrame(
                self.get_data_update(date_last_run, today), columns=columns,
            )
            mid_to_update = tuple(pd.DataFrame(data_new_update).select('MID')['MID'].to_list())
            db_control.execute_no_result(
                'UPDATE "QRISOnboardingLog_TH" SET status=:status, updated_at=now() WHERE mid IN :mid',
                {'status': 'PENDING', 'mid': mid_to_update},
            )
            while len(data_update) > 0:
                processed_data = data_update[:5000]
                filename, df_processed = self.write_data(processed_data, counter, mode='UPDATE')
                if filename is None:
                    break
                mid = tuple(df_processed['MID'].to_list())
                db_control.execute_no_result(
                    'UPDATE "QRISOnboardingLog_TH" SET filename=:filename WHERE mid IN :mid',
                    {'filename': filename, 'mid': mid},
                )
                data_update = data_update[5000:]
                counter += 1

            # data_deleted = self.get_data_deleted(date_last_run, today)
            # while len(data_deleted) > 5000:
            #     self.write_data(data_deleted[:5000])
            #     data_deleted = data_deleted[5000:]
            #     counter += 1

        db_control.execute_no_result(
            'update "PTENControlTable_TN" SET value=:counter WHERE keys=:keys',
            {'counter': counter, 'keys': 'sequence'},
        )
        db_control.execute_no_result(
            'update "PTENControlTable_TN" SET value=:today WHERE keys=:keys',
            {'today': today.strftime('%Y-%m-%d %H:%M'), 'keys': 'last_run_date'},
        )
        return filename

    def upload_to_sftp(self):
        folder_path = self.local_file_path
        files = [f for f in os.listdir(folder_path) if f.endswith('.xlsx')]
        if not files:
            logging.info('No files to upload in %s', folder_path)
            return
        hook = SFTPHook(ssh_conn_id=self.connectionftp)
        remote_dir = self.remote_file_path
        with hook.get_conn() as sftp_client:
            try:
                sftp_client.mkdir(remote_dir)
            except IOError:
                pass
            for f in files:
                local = os.path.join(folder_path, f)
                sftp_client.put(local, f'{remote_dir}/{f}')
                logging.info('Uploaded %s to %s/', f, remote_dir)

    def delete_local_file(self):
        folder_path = self.local_file_path
        for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)
            os.remove(file_path)

    def update_status_log(self):
        db_control = DBConnection(**self.kwargs_db_control_parameter_source)
        db_source = DBConnection(**self.kwargs_db_data_source)
        db_source.execute_no_result_oracle(
            'UPDATE STG_PTEN_MERCHANT_UPDATES SET FLAG=:flag WHERE FLAG<>:flag OR FLAG IS NULL',
            {'flag': 'Y'},
        )
        db_control.execute_no_result(
            'update "QRISOnboardingLog_TH" SET status=:new_status, '
            'last_attempt_at=current_timestamp, attempts=attempts+1, updated_at=now() '
            'WHERE status=:status',
            {'new_status': 'SENT', 'status': 'PENDING'},
        )
        logging.info('Successfully update status onboarding to SENT')
