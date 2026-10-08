"""Posting QR Recon dengan flow TaskFlow dan helper lokal."""

from datetime import datetime, timezone, timedelta
import logging
import os
import sys
from pathlib import Path
import fnmatch
import glob
import shutil
import zipfile
from copy import deepcopy
from types import SimpleNamespace
from typing import List, NamedTuple

from airflow.sdk import dag, task, get_current_context
# Airflow adds the dags directory to sys.path; dags.utils needs its parent.
_project_root = str(Path(__file__).resolve().parents[3])
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from dags.utils.sftp import SftpHook, SFTPHookOcbc
from dags.utils.postgres import PostgresEtlHook
from dags.utils.oracle import OracleEtlHook
from dags.utils.callback import Callback
from dags.utils.telemetry.tracing import tracer
from dags.data_payment.core.model.Models_ReconPWC import ReconHeader, ReconRecordData


class ProcessDataMap(NamedTuple):
    filename: str
    mti_local_filepath: str
    pwc_local_filepath: str
    posting_local_filepaths: List[str]


logger = logging.getLogger(__name__)
sftp = SftpHook()
db_way4 = PostgresEtlHook()
db_source = OracleEtlHook()
db_enrichment = PostgresEtlHook()

config = {'posting_settings': {'space': ' ',
                      'merchant_outlet_spaces': 0,
                      'hs_sequence_spaces': 0,
                      'fields': {'HR': {'institution_ref': 'ID7339', 'tokenization_indicator': 'C'},
                                 'HS': {'batch_type': 'P'},
                                 'DT': {'service_type': '0',
                                        'expiry_date': '',
                                        'authorization_flag': 'A',
                                        'pos_data': '100001154110',
                                        'pos_entry_mode': '012',
                                        'pos_condition_code': '00',
                                        'currency_exponent': '1',
                                        'reversal_reason_code': '',
                                        'replacement_amounts': '',
                                        'service_code': '',
                                        'single_message_indicator': 'Y'},
                                 'OA': {'tip_amount': '',
                                        'cashback_amount': '',
                                        'surcharge_fee': '',
                                        'conversion_rate': '',
                                        'rate_exponent': '',
                                        'rate_date': '',
                                        'reserved_for_future_use': '',
                                        'dcc_indicator': ''},
                                 'TR': {'institution_identification': 'ID7339',
                                        'file_sender': 'ID7339'}}},
 'workflow_name': 'dag_postingfile',
 'owner': 'raymundus.liputre',
 'email_list': [],
 'kwargs_db_source': {'type': 'postgres',
                      'workflow_name': 'workflow',
                      'connection_id': 'db_acq_psql',
                      'chunksize': 10000},
 'db_way4': 'db_rekon_uat',
 'db_source': 'pwcDB',
 'schedule_interval': '0 5-23/2 * * *',
 'start_date': '2023-01-01',
 'tags': ['data_payment', 'acquiring', 'splitfile', 'data_payment', 'core'],
 'source_connection': 'FTP_EBSP',
 'source_path': '/ACQ/MTI/ClearingRintisQRIS/',
 'source_connection_type': 'FTP',
 'destination_connection': 'FTP_MTI',
 'destination_path': '/sftpdata/mti/mti/fromocbc/FileClearing/Rintis/',
 'destination_connection_type': 'FTP',
 'format_date': '%Y%m%d',
 'result_local_path': '/data/airflow/nfs/artifacts/data_payment/acquiring/result/rintis/',
 'result_local_path_PWC': '/data/airflow/nfs/artifacts/data_payment/acquiring/result/rintisPWC',
 'fetch_date': 0,
 'local_path': '/data/airflow/nfs/artifacts/data_payment/acquiring/file/',
 'file_name_mask': 'QR_RECON_*.dsj_ISS',
 'file_name_mask_outgoing': '',
 'custom_function': 'split_rintis_qr_recon',
 'is_split': True,
 'destination_connection_PWC_POST': 'SFTP_PWC_POST',
 'result_local_path_way4': '/data/airflow/nfs/artifacts/data_payment/acquiring/result/way4/',
 'target_path': '/pwrcard/home/usr/data/',
 'target_path_onus': '/pwrcard/home/usr/data/onus/'}

FILE_PATH = os.path.dirname(os.path.realpath(__file__))
workflow_name = config["workflow_name"]
dag_name = workflow_name
callback = Callback(email_list=config["email_list"])

# Manual agar tidak menggandakan pengiriman otomatis dari DAG lama.
user_cron = None
start_date = config["start_date"]
tz = timezone(timedelta(hours=7))
app_name = os.path.basename(os.path.dirname(FILE_PATH))
job_type = "file"
job_operation = "posting"
dynamic_tags = list(dict.fromkeys([
    *config["tags"],
    f"app:{app_name}",
    f"job:{job_type}",
    f"job:{job_type}:{job_operation}",
    f"owner:{config['owner']}",
]))
default_args = {
    "owner": config["owner"],
    "on_failure_callback": callback.task_failure_alert,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "priority_weight": 10,
    "execution_timeout": timedelta(minutes=10),
}



@dag(
    dag_id=workflow_name,
    schedule=user_cron,
    description="Ambil QR Recon, proses posting MTI/PWC, dan kirim hasilnya",
    default_args=default_args,
    start_date=datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=tz),
    catchup=False,
    dagrun_timeout=timedelta(minutes=10),
    tags=dynamic_tags,
    max_active_runs=1,
    on_success_callback=callback.dag_success_alert,
)
def dag_postingfile():
    posting_settings = deepcopy(config['posting_settings'])
    kwargs_db_source = config['kwargs_db_source']

    @task
    @tracer.start_as_current_span(f"{dag_name}_task_get_unprocessed_file")
    def get_unprocessed_files() -> List[str]:
        destination = _hook(config['destination_connection_type'], config['destination_connection'])
        try:
            remote_files = destination.list_directory(config['destination_path'])
            masks = _process_file_mask(config['file_name_mask_outgoing'])
            if any((fnmatch.fnmatch(os.path.basename(name), mask + '.chk') for name in remote_files for mask in masks)):
                logger.info('Marker .chk sudah ada; posting dilewati')
                return []
        finally:
            destination.close_conn()
        source = _hook(config['source_connection_type'], config['source_connection'])
        try:
            files = sorted(source.list_directory(config['source_path']))
            unprocessed = [os.path.basename(name) for name in files if any((fnmatch.fnmatch(os.path.basename(name), mask) for mask in _process_file_mask()))]
        finally:
            source.close_conn()
        logger.info('Unprocessed files: %s', unprocessed)
        return unprocessed

    @task
    @tracer.start_as_current_span(f"{dag_name}_task_process_file")
    def process_file(files: List[str]) -> List[dict]:
        if not files:
            return []
        date = _file_date()
        date_folder = date.strftime('%y/%m/%d')
        local_path = os.path.join(config['local_path'], date_folder)
        os.makedirs(local_path, exist_ok=True)
        source = _hook(config['source_connection_type'], config['source_connection'])
        try:
            for filename in files:
                remote_path = os.path.join(config['source_path'], filename).replace('\\', '/')
                source.retrieve_file(remote_path, os.path.join(local_path, filename))
        finally:
            source.close_conn()
        for archive in glob.glob(os.path.join(local_path, '*.zip')):
            with zipfile.ZipFile(archive) as zipped:
                zipped.extractall(local_path)
        if not config['is_split']:
            logger.info('Split dinonaktifkan')
            return []
        masks = _process_file_mask(config['file_name_mask_outgoing'])
        filenames = sorted((name for name in os.listdir(local_path) if any((fnmatch.fnmatch(name, mask) for mask in masks))))
        mti_path = os.path.join(config['result_local_path'], date_folder)
        pwc_path = os.path.join(config['result_local_path_PWC'], date_folder)
        os.makedirs(mti_path, exist_ok=True)
        os.makedirs(pwc_path, exist_ok=True)
        out_process_datamap = []
        for filename in filenames:
            source_file = os.path.join(local_path, filename)
            mti_file = os.path.join(mti_path, filename)
            pwc_file = os.path.join(pwc_path, filename)
            shutil.copy(source_file, mti_file)
            shutil.copy(source_file, pwc_file)
            way4_records = _get_way4_data_new(period=date.strftime('%Y%m%d'), save_result_file=False)
            posting_records = _map_way4_to_posting_records_new(way4_records)
            posting_files = _split_rintis_qr_recon(str_file_name=source_file, str_result_name=mti_file, str_result_pwc=pwc_file, way4_posting_records=posting_records)
            out_process_datamap.append(ProcessDataMap(filename, mti_file, pwc_file, posting_files)._asdict())
        for raw_file in glob.glob(os.path.join(config['result_local_path_PWC'], 'QR_RECON*')):
            os.remove(raw_file)
        return out_process_datamap

    @task
    @tracer.start_as_current_span(f"{dag_name}_task_send_file")
    def send_file(processed_files: List[dict]):
        if not processed_files:
            return
        mti = _hook(config['destination_connection_type'], config['destination_connection'])
        try:
            pwc = SFTPHookOcbc(ssh_conn_id=config['destination_connection_PWC_POST'])
            try:
                for result in processed_files:
                    remote_mti = os.path.join(config['destination_path'], result['filename']).replace('\\', '/')
                    mti.store_file(remote_mti, result['mti_local_filepath'])
                    for filepath in result['posting_local_filepaths']:
                        remote_pwc = os.path.join(config['target_path'], os.path.basename(filepath)).replace('\\', '/')
                        pwc.store_file(remote_pwc, filepath)
                    marker = result['mti_local_filepath'] + '.chk'
                    with open(marker, 'w', encoding='utf-8'):
                        pass
                    mti.store_file(remote_mti + '.chk', marker)
                    logger.info('Posting terkirim: %s', result['filename'])
            finally:
                pwc.close_conn()
        finally:
            mti.close_conn()

    def _processing_date(context):
        date = context.get('logical_date')
        if date is None:
            date = context['dag_run'].run_after
        return date

    def _file_date():
        return _processing_date(get_current_context()) + timedelta(days=config['fetch_date'])

    def _process_file_mask(outgoing_file_name=''):
        date = _file_date() + timedelta(hours=9)
        mask = outgoing_file_name or config['file_name_mask']
        return mask.replace('.dsj', date.strftime(config['format_date'])).split('|')

    def _hook(connection_type, connection_id):
        return sftp.get_hook(connection_type, connection_id)

    def _get_way4_data_new(period=None, save_result_file=True):
        con_acq = None
        try:
            kwargs_db_source = config.get('kwargs_db_source')
            timestamp = _get_timestamp_POST()
            if period is None:
                if len(timestamp) < 8 or not timestamp[:8].isdigit():
                    raise ValueError(f'Format timestamp POST tidak valid: {timestamp!r}')
                period = timestamp[:8]
            yesterday = datetime.today() - timedelta(days=1)
            yesterday = yesterday.strftime('%Y%m%d')
            sql = """
                SELECT
                    merchantid as merchant_id,
                    institutionbranch_acq as terminal_id,
                    rrn as retrieval_reference_number,
                    period as transaction_date,
                    periodtime as transaction_time,
                    transactionamount as transaction_amount,
                    transactioncurrency as transaction_amount_currency,
                    '00000000028' as acquiring_bank_code,
                    '00000000028' as issuer_bank_code,
                    '' as forwarding_institution,
                    pan as customer_pan,
                    rc as response_code,
                    case
                        when TransactionType = 'CH Payment' then 'D'
                        else 'C'
                    end as transaction_sign,
                    auth_code as approval_code,
                    reversalseq,
                    sourceregnum as srn,
                    stan
                FROM
                    sw_replicate.on_doc_transaction odt
                WHERE
                    connection_acq = 'T'
                    and connection_iss in ('ACQQR', 'a', 'O')
                    and PAN like %(pan_prefix)s
                    and transactiontype in ('CH Payment', 'Credit')
                    and period = %(yesterday)s
                    and (
                        odt.rc::int = 0
                        or (
                            odt.rc::int in (68, 82)
                            and exists (
                                select 1 from sw_replicate.on_doc_transaction r
                                where r.rrn = odt.rrn
                                and r.transactiontype = 'Retail'
                                and r.rc::int = 0
                                and r.reversalseq::int = 0
                            )
                        )
                    )
                ORDER BY
                    transaction_time DESC
            """
            logging.info('============ START GET DATA WAY4 ============')
            logging.info('period: %s', period)
            logging.info('sql: %s', sql)
            logging.info('connection db way4')
            records = db_way4.get_records(sql=sql, connection_id=config.get('db_way4'), params={'yesterday': yesterday, 'pan_prefix': '936000281%'})
            if records is None:
                logging.warning('way4 get data return None | period=%s | con_acq=%s', period, con_acq)
                records = []
            if records and save_result_file:
                os.makedirs(config['result_local_path_way4'], exist_ok=True)
                spek_name_pwc = f'POSTFLIN_{timestamp}.txt'
                result_file = os.path.join(config['result_local_path_way4'], spek_name_pwc)
                logging.info('Save way4 result to file:%s', result_file)
                with open(result_file, 'w') as file:
                    for row in records:
                        file.write('|'.join(('' if value is None else str(value) for value in row)))
                        file.write('\n')
                logging.info('way4 result file created:%s | total records:%s', result_file, len(records))
            return records
        except Exception as e:
            logging.exception('Gagal mengambil data WAY4 | period=%s | con_acq=%s | error: %s', period, con_acq, e)
            raise

    def _posting_space():
        value = posting_settings.get('space', ' ')
        if not isinstance(value, str) or len(value) != 1 or (not value.isascii()) or (not value.isprintable()):
            raise ValueError('posting_settings.space harus satu karakter ASCII yang dapat dicetak')
        return value

    def _posting_field(record, name, length):
        try:
            value = posting_settings['fields'][record][name]
        except KeyError as exc:
            raise ValueError(f'Setting {record}.{name} belum diisi pada posting_settings.fields') from exc
        if not isinstance(value, str) or not value.isascii() or (value and (not value.isprintable())):
            raise ValueError(f'Setting {record}.{name} harus string ASCII satu baris')
        if len(value) > length:
            raise ValueError(f'Setting {record}.{name} melebihi panjang field {length}')
        return value.ljust(length, _posting_space())

    def _posting_spaces(name):
        count = posting_settings.get(name, 0)
        if type(count) is not int or count < 0:
            raise ValueError(f'posting_settings.{name} harus integer >= 0')
        return _posting_space() * count

    def _merchant_outlet_space():
        return _posting_spaces('merchant_outlet_spaces')

    def _hs_sequence_space():
        return _posting_spaces('hs_sequence_spaces')

    def _split_rintis_qr_recon(str_file_name, str_result_name, str_result_pwc, way4_posting_records=None):
        try:
            logging.info('========== SPLIT PROCESSED START QR RECON ==========')
            file_unprocessed = open(str_file_name, 'r')
            file_processed = open(str_result_name, 'w')
            file_processed_PWC = open(str_result_pwc, 'w')
            merchants = _get_data_merchant_pwc_qr_acceptor()
            merchants_pwcs = set((str(row[0]).strip() for row in merchants))
            logging.info('pwc acceptor point: %s', merchants)
            logging.info('merchant_pwcs: %s', merchants_pwcs)
            row_count = 0
            row_count_pwc = 0
            record = 0
            record_pwc = 0
            hd = None
            generated_posting_files = []
            pwc_posting_records = []
            way4_posting_records = list(way4_posting_records or [])
            posting_filename = None
            for line in file_unprocessed:
                logging.info('Line: %s', line)
                if line.startswith('RH'):
                    logging.info('===== START RH RECORD =====')
                    hd = ReconHeader.parse_header(line)
                    file_processed.write(line)
                    file_processed_PWC.write(hd.to_header_line() + '\n')
                    logging.info('===== END RH RECORD =====')
                elif line.startswith('DH'):
                    logging.info('=== START DH RECORD ===')
                    rec = ReconRecordData.parse_recon_data_line(line)
                    mpan = str(rec.merchant_pan).strip()
                    logging.info('=== END DH RECORD ===')
                    if mpan in merchants_pwcs:
                        logging.info('========== START CREATE FILE SPLIT ==========')
                        logging.info('Merchants is PWC')
                        mid_records = _get_merchant_by_acceptor_point(mpan)
                        if not mid_records:
                            logging.info('MID not found for MPAN [%s]', mpan)
                            continue
                        mid = mid_records[0][0]
                        logging.info('MID record:%s', mid_records)
                        mid = str(mid).strip()
                        logging.info('[QR_RECON][SPLIT] MID dipilih | MPAN=%s | MID=%s | tipe=%s | panjang=%s', mpan, mid, type(mid).__name__, len(mid))
                        filename_datetime = _get_timestamp_POST()
                        if posting_filename is None:
                            posting_filename = f'POSTFLIN_{filename_datetime}.txt'
                        filename = posting_filename
                        logging.info('[QR_RECON] Calling create_posting_file | filename:%s | mpan:%s | mid:%s | target_split:%s ', filename, mpan, mid, str_result_pwc)
                        if len(mid_records[0]) < 2 or mid_records[0][1] is None:
                            raise ValueError(f'MERCHANT_NUMBER PWC tidak ditemukan untuk MPAN [{mpan}]')
                        merchant_number = str(mid_records[0][1]).strip()
                        if not merchant_number.isascii() or not merchant_number.isdigit() or len(merchant_number) > 15:
                            raise ValueError(f'MERCHANT_NUMBER PWC harus berupa angka maksimal 15 digit untuk MPAN [{mpan}]')
                        posting_rec = SimpleNamespace(**rec.dict(), merchant_number=merchant_number)
                        pwc_posting_records.append((posting_rec, mid))
                        logging.info('[QR_RECON][SPLIT] Merchant siap untuk posting | MPAN=%s | MID(MER_ACCEPTOR_POINT_ID)=%s | MERCHANT_NUMBER=%s | panjang_merchant_number=%s | HS.outlet_number=MID | HS.merchant_number=MERCHANT_NUMBER', mpan, mid, merchant_number, len(merchant_number))
                        file_processed_PWC.write(rec.to_line() + '\n')
                        row_count_pwc += 1
                        logging.info('rec_to_line:%s', rec.to_line())
                        logging.info('filename_split:%s', filename)
                        logging.info('=== END CREATE FILE SPLIT ===')
                    else:
                        logging.info('merchant MTI')
                        file_processed.write(line)
                        row_count += 1
                elif line.startswith('RT'):
                    file_processed.write(line[:28] + str(row_count) + '|' + str(record))
                    file_processed_PWC.write(hd.to_header_line() + '|' + str(record_pwc))
                    pwc_posting_records.extend(way4_posting_records)
                    if posting_filename is None and pwc_posting_records:
                        posting_filename = f'POSTFLIN_{_get_timestamp_POST()}.txt'
                    if pwc_posting_records:
                        generated_posting_file = _create_posting_batch_file(posting_filename, pwc_posting_records, target_split=str_result_pwc, header=hd)
                        generated_posting_files.append(generated_posting_file)
                    logging.info('==========SPLIT PROCESSED END QR RECO ==========')
            return generated_posting_files
        except Exception:
            logging.info('Error split rintis RAW')
            raise

    def _get_data_merchant_pwc_qr_acceptor():
        logging.info('START Get Data merchant PWC')
        logging.info(f'get_data: kwargs_db_source={kwargs_db_source}')
        try:
            merchant_query = 'SELECT AP_ON_ID_10 FROM MER_ACCEPTOR_POINT'
            logging.info('get data merchant pwc')
            logging.info('executing query: %s', merchant_query)
            logging.info('connection db PWC')
            records = db_source.get_records(sql=merchant_query, connection_id=config.get('db_source'))
            if records is None:
                logging.warning('pwc get data return None')
                records = []
            logging.info('final result ap_on_id: %s', records)
            return records
        except Exception as e:
            logging.exception('Gagal mengambil data pwc')
            raise

    def _get_merchant_by_acceptor_point(mpan: str):
        try:
            sql = """
                SELECT MER_ACCEPTOR_POINT_ID, MERCHANT_NUMBER FROM MER_ACCEPTOR_POINT WHERE TRIM(AP_ON_ID_10) = TRIM(:mpan)
            """
            mpan = str(mpan).strip()
            logging.info('[QR_RECON][LOOKUP] Mulai lookup MID dan MERCHANT_NUMBER | tabel=MER_ACCEPTOR_POINT | MPAN=%s', mpan)
            logging.info('sql:%s', sql)
            logging.info('mpan acceptor:%s', mpan)
            records = db_source.get_records(sql=sql, connection_id=config.get('db_source'), params={'mpan': mpan})
            if records is None:
                logging.warning('pwc get data return None')
                records = []
            logging.info('[QR_RECON][LOOKUP] Selesai | MPAN=%s | jumlah_baris=%s | urutan_kolom=(MER_ACCEPTOR_POINT_ID, MERCHANT_NUMBER) | hasil=%s', mpan, len(records), records)
            if not records:
                logging.warning('[QR_RECON][LOOKUP] Merchant tidak ditemukan | MPAN=%s', mpan)
            return records
        except Exception as e:
            logging.exception('[QR_RECON][LOOKUP] Gagal mengambil MID dan MERCHANT_NUMBER dari PWC | MPAN=%s', mpan)
            raise

    def _create_posting_batch_file(filename, posting_records, target_split: str, header=None):
        if not filename or not filename.endswith('.txt'):
            raise ValueError(f'Invalid posting filename: {filename!r}')
        if '..' in filename or '/' in filename or '\\' in filename:
            raise ValueError('filename cannot contain a path')
        full_path = os.path.join(os.path.dirname(target_split), filename)
        grouped_records = {}
        for rec, mid in posting_records:
            normalized_mid = str(mid).strip()
            group_key = (normalized_mid, str(rec.merchant_pan).strip(), str(rec.terminal_id).strip())
            grouped_records.setdefault(group_key, []).append(rec)
        sequence_number = 1
        transaction_sequence = 0
        voucher_sequence = 0
        posting_lines = [_generate_hr_record(header).rstrip('\n')]
        for (mid, _mpan, _terminal_id), records in grouped_records.items():
            first_rec = records[0]
            transaction_sequence = 0
            sequence_number += 1
            posting_lines.append(_generate_hs_record(first_rec, mid, sequence_number, merchant_number=_posting_merchant_number(first_rec), outlet_number=str(getattr(first_rec, 'outlet_number', mid))).rstrip('\n'))
            for rec in records:
                transaction_sequence += 1
                voucher_sequence += 1
                sequence_number += 1
                posting_lines.append(_generate_dt_record(rec, sequence_number, transaction_sequence, voucher_sequence).rstrip('\n'))
                sequence_number += 1
                posting_lines.append(_generate_oa_record(rec, sequence_number, voucher_sequence).rstrip('\n'))
            sequence_number += 1
            posting_lines.append(_generate_ts_record(first_rec, mid, sequence_number, group_records=records).rstrip('\n'))
        sequence_number += 1
        posting_lines.append(_generate_tr_record(sequence_number, posting_lines))
        full_containt = '\n'.join(posting_lines)
        expected_lengths = {'HR': 47, 'HS': 89 + len(_merchant_outlet_space()) + len(_hs_sequence_space()), 'DT': 147, 'OA': 184, 'TS': 133 + len(_merchant_outlet_space()) + len(_hs_sequence_space()), 'TR': 74}
        for line in posting_lines:
            expected_length = expected_lengths.get(line[:2])
            if expected_length is None or len(line) != expected_length:
                raise ValueError(f'Invalid {line[:2]} record length: {len(line)}, expected {expected_length}')
        with open(full_path, 'w', encoding='utf-8') as generated_file:
            generated_file.write(full_containt)
        logging.info('Posting batch generated: %s | groups=%s | transactions=%s | records=%s', full_path, len(grouped_records), voucher_sequence, len(posting_lines))
        return full_path

    def _generate_hr_record(header=None):
        record_type = 'HR'
        last_sequence_number = 0
        this_sequence = last_sequence_number + 1
        sequence_name = _pad_with_spaces(_to_padded_number(8, this_sequence), 8)
        bank_code_id = _posting_field('HR', 'institution_ref', 6)
        institution_ref = _pad_with_spaces(bank_code_id, 6)
        today_yymmdd = _pad_with_spaces(_get_today_yymmdd(), 6)
        ptidn = _pad_with_spaces(_to_padded_number(6, this_sequence), 6)
        file_ref_value = f'PTIN{ptidn}{today_yymmdd}'
        file_ref = _pad_with_spaces(file_ref_value, 16)
        recon_date = str(header.recon_file_date).strip() if header else ''
        processDate = recon_date + datetime.now().strftime('%H%M%S') if len(recon_date) == 8 else _get_timestamp_POST()
        tokenization_indicator = _posting_field('HR', 'tokenization_indicator', 1)
        result = record_type + sequence_name + institution_ref + file_ref + processDate + tokenization_indicator
        if len(result) != 47:
            raise ValueError(f'invalid HR RECORD length:{len(result)}, expected 47')
        return result + '\n'

    def _generate_tr_record(sequence_number=1, posting_lines=None):
        record_type = 'TR'
        last_sequence_number = 0
        this_sequence = last_sequence_number + 1
        sequence_in_file = _to_padded_number(8, sequence_number)
        institution_identification = _posting_field('TR', 'institution_identification', 6)
        file_sender = _posting_field('TR', 'file_sender', 6)
        posting_lines = posting_lines or []
        dt_lines = [line for line in posting_lines if line.startswith('DT')]
        debit_lines = [line for line in dt_lines if len(line) >= 113 and line[112] == 'D']
        credit_lines = [line for line in dt_lines if len(line) >= 113 and line[112] == 'C']
        count_debit = _to_padded_number(8, len(debit_lines))
        count_credit = _to_padded_number(8, len(credit_lines))
        amount_debit_value = sum((int(line[94:112].strip() or '0') for line in debit_lines))
        amount_credit_value = sum((int(line[94:112].strip() or '0') for line in credit_lines))
        amount_debit = str(amount_debit_value).zfill(18)
        amount_credit = str(amount_credit_value).zfill(18)
        result = record_type + sequence_in_file + institution_identification + file_sender + count_debit + count_credit + amount_credit + amount_debit
        if len(result) != 74:
            raise ValueError(f'invalid TR RECORD length:{len(result)}, expected 74')
        return result

    def _posting_merchant_number(rec, merchant_number=None):
        value = merchant_number if merchant_number is not None else getattr(rec, 'merchant_number', None)
        if value is None:
            raise ValueError('MERCHANT_NUMBER belum tersedia pada record posting; isi dari lookup merchant PWC')
        value = str(value).strip()
        if not value or not value.isascii() or (not value.isdigit()) or (len(value) > 15):
            raise ValueError(f'MERCHANT_NUMBER harus angka maksimal 15 digit: {value!r}')
        return value

    def _generate_hs_record(rec: ReconRecordData, mid: str, sequence_number=1, *, merchant_number=None, outlet_number=None):
        logging.info('========== GENERATE HS RECORD ==============')
        logging.info('MID:%s', mid)
        logging.info('REC:%s', rec)
        logging.info('Terminal ID :%s', rec.terminal_id)
        record_type = 'HS'
        last_sequence = 0
        new_sequence = last_sequence + 1
        last_batch = 0
        new_batch = last_batch + 1
        record_sequence = _to_padded_number(8, sequence_number)
        record_sequences = _pad_with_spaces(record_sequence, 8)
        merchant_number_value = _posting_merchant_number(rec, merchant_number)
        merchant_number = _pad_with_spaces(merchant_number_value, 15)
        outlet_value = str(getattr(rec, 'outlet_number', mid))
        outlet_number = _pad_with_spaces(outlet_value[:15], 15)
        terminal_id = _pad_with_spaces(rec.terminal_id[:15], 15)
        batch_number = _to_padded_number(8, new_batch)
        capture_date = str(getattr(rec, 'batch_capture_date', _get_today_yyyymmdd()))
        batch_capture_date = _pad_with_spaces(capture_date[:8], 8)
        batch_datetime = _pad_with_spaces(f'{rec.transaction_date}{rec.transaction_time}', 14)
        batch_currency_value = str(getattr(rec, 'batch_currency', rec.transaction_amount_currency))
        batch_currency = _pad_with_spaces(batch_currency_value[:3], 3)
        batch_type = _posting_field('HS', 'batch_type', 1)
        result = record_type + record_sequence + _hs_sequence_space() + merchant_number + _merchant_outlet_space() + outlet_number + terminal_id + batch_number + batch_capture_date + batch_datetime + batch_currency + batch_type
        logging.info('HS final result:[%s]', result)
        logging.info('HS LENGHT = %s', len(result))
        expected_length = 89 + len(_merchant_outlet_space()) + len(_hs_sequence_space())
        if len(result) != expected_length:
            raise ValueError(f'invalid HS RECORD length:{len(result)}, expected {expected_length}')
        return result + '\n'

    def _generate_dt_record(rec: ReconRecordData, sequence_number=1, transaction_sequence=1, voucher_sequence=None):
        record_type = 'DT'
        last_sequence = 0
        new_sequence = last_sequence + 1
        last_trx_batch = 0
        new_trx_batch = last_trx_batch + 1
        voucher_number_sequence = transaction_sequence if voucher_sequence is None else voucher_sequence
        voucher_generated = _to_padded_number(8, voucher_number_sequence)
        record_sequence_in_file = _to_padded_number(8, sequence_number)
        transaction_sequence_in_batch = _to_padded_number(7, transaction_sequence)
        service_type = _posting_field('DT', 'service_type', 1)
        voucher_number = _pad_with_spaces(voucher_generated[:8], 8)
        card_number = _pad_with_spaces(rec.customer_pan[:22], 22)
        expiry_date = _posting_field('DT', 'expiry_date', 6)
        processing_date = _pad_with_spaces(rec.processing_code[:6], 6)
        reversal_flag = str(getattr(rec, 'reversal_flag', 'N'))[:1]
        authorization_flag = _posting_field('DT', 'authorization_flag', 1)
        post_date = _posting_field('DT', 'pos_data', 12)
        post_entry_mode = _posting_field('DT', 'pos_entry_mode', 4)
        post_condition_code = _posting_field('DT', 'pos_condition_code', 2)
        transaction_datetime = _pad_with_spaces(f'{rec.transaction_date}{rec.transaction_time}', 14)
        transaction_amount = _to_fixed_numeric(rec.transaction_amount[:18], 18)
        transaction_sign = 'D' if rec.processing_code[:2] in {'20', '29'} else 'C'
        transaction_currency = _pad_with_spaces(rec.transaction_amount_currency[:3], 3)
        currency_exponent = _posting_field('DT', 'currency_exponent', 1)
        reversal_reason_code = _posting_field('DT', 'reversal_reason_code', 2)
        replacement_amounts = _posting_field('DT', 'replacement_amounts', 18)
        authorization_code = _pad_with_spaces(rec.approval_code[:6], 6)
        service_code = _posting_field('DT', 'service_code', 3)
        single_message_indicator = _posting_field('DT', 'single_message_indicator', 1)
        result = record_type + record_sequence_in_file + transaction_sequence_in_batch + service_type + voucher_number + card_number + expiry_date + processing_date + reversal_flag + authorization_flag + post_date + post_entry_mode + post_condition_code + transaction_datetime + transaction_amount + transaction_sign + transaction_currency + currency_exponent + reversal_reason_code + replacement_amounts + authorization_code + service_code + single_message_indicator
        if len(result) != 147:
            raise ValueError(f'invalid DT RECORD length:{len(result)}, expected 147')
        return result + '\n'

    def _generate_oa_record(rec: ReconRecordData, sequence_number=1, voucher_sequence=1):
        record_type = 'OA'
        last_sequence = 0
        new_sequence = last_sequence + 1
        sequence = _to_padded_number(8, sequence_number)
        record_sequence = sequence
        voucher_number = _to_padded_number(8, voucher_sequence)
        tip_amount = _posting_field('OA', 'tip_amount', 18)
        cashback_amount = _posting_field('OA', 'cashback_amount', 18)
        fee = _to_fixed_numeric(rec.convenience_fee[:18], 18)
        surcharge_fee = _posting_field('OA', 'surcharge_fee', 18)
        billing_amount = _to_fixed_numeric(rec.transaction_amount[:18], 18)
        billing_currency = _pad_with_spaces(rec.transaction_amount_currency[:3], 3)
        conversion_rate = _posting_field('OA', 'conversion_rate', 12)
        rate_exponent = _posting_field('OA', 'rate_exponent', 2)
        rate_date = _posting_field('OA', 'rate_date', 14)
        reversed_for_future_use = _posting_field('OA', 'reserved_for_future_use', 9)
        external_ref_id = _pad_with_spaces(rec.invoice_data[:23], 23)
        dcc_indicator = _posting_field('OA', 'dcc_indicator', 1)
        reversed_for_future_use2 = _pad_with_spaces(rec.retrieval_reference_number[:12], 12)
        result = record_type + record_sequence + voucher_number + tip_amount + cashback_amount + fee + surcharge_fee + billing_amount + billing_currency + conversion_rate + rate_exponent + rate_date + reversed_for_future_use + external_ref_id + dcc_indicator + reversed_for_future_use2
        if len(result) != 184:
            raise ValueError(f'invalid OA RECORD length:{len(result)}, expected 184')
        return result + '\n'

    def _generate_ts_record(rec: ReconRecordData, mid: str, sequence_number=1, group_records=None):
        record_type = 'TS'
        last_sequence = 0
        new_sequence = last_sequence + 1
        last_batch = 0
        new_batch = last_batch + 1
        sequence_in_file = _to_padded_number(8, sequence_number)
        merchant_number_value = _posting_merchant_number(rec)
        merchant_number = _pad_with_spaces(merchant_number_value, 15)
        outlet_value = str(getattr(rec, 'outlet_number', mid))
        outlet_number = _pad_with_spaces(outlet_value[:15], 15)
        terminal_id = _pad_with_spaces(rec.terminal_id[:15], 15)
        batch_number = _to_padded_number(8, new_batch)
        batch_capture_date = _pad_with_spaces(_get_today_yyyymmdd(), 8)
        batch_datetime = _pad_with_spaces(f'{rec.transaction_date}{rec.transaction_time}', 14)
        records = group_records or [rec]
        debit_records = [r for r in records if r.processing_code[:2] in {'20', '29'}]
        credit_records = [r for r in records if r.processing_code[:2] not in {'20', '29'}]
        record_count_debit = _to_padded_number(6, len(debit_records))
        net_amount_debit = _to_fixed_numeric(sum((int(r.transaction_amount) for r in debit_records)), 18)
        record_count_credit = _to_padded_number(6, len(credit_records))
        net_amount_credit = _to_fixed_numeric(sum((int(r.transaction_amount) for r in credit_records)), 18)
        result = record_type + sequence_in_file + _hs_sequence_space() + merchant_number + _merchant_outlet_space() + outlet_number + terminal_id + batch_number + batch_capture_date + batch_datetime + record_count_debit + net_amount_debit + record_count_credit + net_amount_credit
        expected_length = 133 + len(_merchant_outlet_space()) + len(_hs_sequence_space())
        if len(result) != expected_length:
            raise ValueError(f'invalid TS RECORD length:{len(result)}, expected {expected_length}')
        return result + '\n'

    def _get_timestamp_POST():
        try:
            return datetime.now().strftime('%Y%m%d%H%M%S')
        except Exception as e:
            raise RuntimeError(f'Failed to generate timestamp: {e}')

    def _get_today_yymmdd():
        try:
            today = datetime.today()
            return today.strftime('%y%m%d')
        except Exception as e:
            return f'Error: {str(e)}'

    def _get_today_yyyymmdd():
        try:
            today = datetime.today()
            return today.strftime('%Y%m%d')
        except Exception as e:
            raise RuntimeError(f"Failed to get today's date: {e}")

    def _pad_with_spaces(text: str, length: int):
        if not isinstance(text, str):
            raise TypeError('text parameter must be string.')
        if not isinstance(length, int):
            raise TypeError('length parameter must be integer.')
        if length < 0:
            raise ValueError('length parameter cannot negatif.')
        if len(text) >= length:
            return text
        spaces_needed = length - len(text)
        return text + _posting_space() * spaces_needed

    def _to_padded_number(length: int, number: int):
        try:
            if not isinstance(length, int) or not isinstance(number, int):
                raise TypeError('length and number must be integer type.')
            if length <= 0:
                raise ValueError('length must be positive number.')
            num_str = str(abs(number))
            if len(num_str) > length:
                raise ValueError('length number greater than length requested')
            return num_str.zfill(length)
        except (TypeError, ValueError):
            raise

    def _to_fixed_numeric(value, length: int):
        """Normalisasi field NAM menjadi angka zero-padded dengan panjang tetap."""
        text_value = str(value).strip()
        if text_value[:1] in {'C', 'D'}:
            text_value = text_value[1:]
        if not text_value:
            text_value = '0'
        if not text_value.isdigit():
            raise ValueError(f'NAM value must contain digits only: {value!r}')
        if len(text_value) > length:
            raise ValueError(f'NAM value length {len(text_value)} exceeds requested length {length}')
        return text_value.zfill(length)

    def _map_way4_to_posting_records_new(records):
        columns = ('merchant_id', 'terminal_id', 'retrieval_reference_number', 'transaction_date', 'transaction_time', 'transaction_amount', 'transaction_amount_currency', 'acquiring_bank_code', 'issuer_bank_code', 'forwarding_institution', 'customer_pan', 'response_code', 'transaction_sign', 'approval_code', 'reversalseq', 'srn', 'stan')
        records = _get_data_qris_enrichment(records, columns)

        def text(value, default=''):
            return default if value is None else str(value).strip()

        def numeric(value, length):
            if value is None:
                return '0'.zfill(length)
            value_text = format(value, 'f') if hasattr(value, 'as_tuple') else str(value)
            value_text = value_text.strip()
            if '.' in value_text:
                integer, fraction = value_text.split('.', 1)
                if fraction.strip('0'):
                    value_text = str(int(round(float(value_text))))
                else:
                    value_text = integer
            value_text = value_text.lstrip('+') or '0'
            if not value_text.isdigit():
                raise ValueError(f'Invalid Way4 numeric value: {value!r}')
            return value_text[-length:].zfill(length)

        def processing_code(TransactionSign, from_account, to_account):
            from_account = text(from_account)
            to_account = text(to_account)
            if not from_account or not to_account:
                raise ValueError(f'Invalid processing code inputs: from_account={from_account!r}, to_account={to_account!r}')
            if len(from_account) != 2 or not from_account.isdigit():
                raise ValueError(f'Invalid from_account, expected 2-digit code: {from_account!r}')
            if len(to_account) != 2 or not to_account.isdigit():
                raise ValueError(f'Invalid to_account, expected 2-digit code: {to_account!r}')
            ProcessingCode = '20'
            if TransactionSign == 'C':
                ProcessingCode = '26'
            return ProcessingCode + from_account + to_account
        posting_records = []
        for values in records or []:
            mid = text(values.get('merchant_id'))
            if not mid:
                logging.warning('Skip Way4 record without mid: %s', values)
                continue
            period = text(values.get('transaction_date'))
            settlement_date = text(values.get('transaction_date'))
            transaction_date = text(values.get('transaction_date'))
            transaction_time = text(values.get('transaction_time'), '000000')
            transaction_time = transaction_time[:6].zfill(6)
            settlement_currency = text(values.get('transaction_amount_currency'), '360')[:3].ljust(3)
            transaction_currency = text(values.get('transaction_amount_currency'), settlement_currency)[:3].ljust(3)
            cpan = text(values.get('customer_pan'))
            rrn = text(values.get('retrieval_reference_number'))
            source_recnum = text(values.get('srn'))
            institution_acq = text(values.get('acquiring_bank_code'))
            merchant_id = text(values.get('merchant_id'), mid)
            authorization_code = text(values.get('approval_code'))
            response_code = text(values.get('response_code'), '00')
            reversal_sequence = values.get('reversalseq')
            reversal_flag = 'F' if reversal_sequence is not None and int(reversal_sequence) > 0 else 'N'
            terminal_id = text(values.get('terminal_id'))
            issuer_bank_code = text(values.get('issuer_bank_code'))
            merchant_criteria = text(values.get('merchant_criteria'))
            from_account_type = text(values.get('from_account_type'))
            transaction_sign = text(values.get('transaction_sign'))
            to_account_type = text(values.get('to_account_type'))
            mpan = text(values.get('mpan'))
            merchant_type = text(values.get('merchant_type'))
            invoice_number = text(values.get('invoice_number'))
            base_record = ReconRecordData(recon_header='DH', terminal_id=terminal_id[:16].ljust(16), retrieval_reference_number=rrn[:12].ljust(12), merchant_pan=mpan[:19].ljust(19), transaction_date=transaction_date, transaction_time=transaction_time, processing_code=processing_code(transaction_sign, from_account_type, to_account_type), transaction_amount=numeric(values.get('transaction_amount'), 12), convenience_fee=numeric(values.get('fee'), 9), transaction_amount_currency=transaction_currency, merchant_type=merchant_type[:4].ljust(4), merchant_criteria=merchant_criteria[:3].ljust(3), acquiring_bank_code=institution_acq, issuer_bank_code=issuer_bank_code, forwarding_institution_id=' ' * 11, response_code=response_code[:2].ljust(2), customer_pan=cpan[:28].ljust(28), invoice_data=invoice_number[:20].ljust(20), approval_code=authorization_code[:6].ljust(6), message_type_indicator='0200')
            merchant_rows = _get_merchant_by_acceptor_point(mpan)
            if not merchant_rows or len(merchant_rows[0]) < 2 or merchant_rows[0][1] is None:
                raise ValueError(f'MERCHANT_NUMBER PWC tidak ditemukan untuk MPAN [{mpan}]')
            merchant_number = _posting_merchant_number(base_record, merchant_rows[0][1])
            rec = SimpleNamespace(**base_record.dict(), merchant_number=merchant_number, outlet_number=merchant_id, batch_capture_date=period, batch_currency=transaction_currency, reversal_flag=reversal_flag, stan=text(values.get('stan')), transactiontype=text(values.get('transactiontype')))
            posting_records.append((rec, mid))
        return posting_records

    def _get_data_qris_enrichment(records, columns):
        logging.info('START Get Data QRIS Enrichment')
        if not kwargs_db_source:
            logging.info('kwargs_db_source is None or empty. Check Operator initialization in DAG.')
        if not isinstance(kwargs_db_source, dict):
            logging.error(f'kwargs_db_source must be a dict, got {type(kwargs_db_source)}')
            raise ValueError('kwargs_db_source must be a dictionary.')
        records = [dict(zip(columns, r)) if not isinstance(r, dict) else r for r in records]
        rrns = [r['retrieval_reference_number'] for r in records]
        qris_data = {}
        chunk_size = 5000
        for i in range(0, len(rrns), chunk_size):
            batch = rrns[i:i + chunk_size]
            get_qris_query = """
                select
                    qtlt.rrn,
                    mt.merchant_criteria,
                    qtlt.from_account_type,
                    qtlt.to_account_type,
                    qtlt.mpan,
                    qtlt.merchant_type,
                    qtlt.invoice_number,
                    qtlt.fee
                from qris_transaction_log_tt qtlt
                join merchant_tm mt on qtlt.mid = mt.mid
                where qtlt.rrn = any(%(rrns)s)
            """
            logging.info(f'Executing query batch {i // chunk_size + 1}, size: {len(batch)}')
            rows = db_enrichment.get_records(sql=get_qris_query, connection_id=kwargs_db_source['connection_id'], params={'rrns': batch})
            for row in rows:
                rrn = row[0]
                if rrn not in qris_data:
                    qris_data[rrn] = {'merchant_criteria': row[1], 'from_account_type': row[2], 'to_account_type': row[3], 'mpan': row[4], 'merchant_type': row[5], 'invoice_number': row[6], 'fee': row[7]}
        for r in records:
            match = qris_data.get(r['retrieval_reference_number'])
            if match:
                r.update(match)
        logging.info(f'Finish Get Data QRIS Enrichment, total matched: {len(qris_data)}')
        return records
    files = get_unprocessed_files()
    processed_files = process_file(files)
    send_file(processed_files)


postingfile_dag = dag_postingfile()
