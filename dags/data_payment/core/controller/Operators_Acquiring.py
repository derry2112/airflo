import sys
import pandas as pd
sys.path.insert(1, '/data/airflow/nfs/dags')
from airflow.operators.python import get_current_context
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.ftp.hooks.ftp import FTPHook
# from airflow import AirflowException
from pathlib import Path
from types import SimpleNamespace
from data_payment.core.model.Models_DisputePWC import DisputeRecord, SEP, DisputeHeader
# Keterangan: import lama dikomentari karena file dari foto bernama Models_ReconPWC.py.
from data_payment.core.model.Models_ReconPWC import ReconHeader, ReconRecordData
import glob, os, logging, zipfile, fnmatch, shutil, datetime, re
from data_payment.core.connection.Connection4WAY import Way4DB, PwcDB


class Replication(object):
    def __init__(self, **kwares):
        self.config = kwares
        if self.config is None:
            print("Config is not defined!!!!")
        self.workflow_name = self.config.get('workflow_name')
        self.source_connection = self.config.get('vendor')
        self.source_connection = self.config.get('source_connection')
        self.source_path = self.config.get('source_path')
        self.source_connection_type = self.config.get('source_connection_type')
        self.destination_connection = self.config.get('destination_connection')
        self.destination_path = self.config.get('destination_path')
        self.destination_connection_type = self.config.get('destination_connection_type')
        self.format_date = self.config.get('format_date')
        self.local_path = self.config.get('local_path')
        self.result_local_path = self.config.get('result_local_path')
        self.fetch_date = self.config.get('fetch_date')
        self.file_name_mask = self.config.get('file_name_mask')
        self.file_name_mask_outgoing = self.config.get('file_name_mask_outgoing')
        self.is_split = self.config.get('is_split')
        self.custom_function = self.config.get('custom_function')

        #deny
        self.target_path = self.config.get('target_path')
        self.destination_connection_PWC_POST = self.config.get('destination_connection_PWC_POST')
        self.result_local_way4 = self.config.get('result_local_path_way4')
        self.target_path_onus = self.config.get('target_path_onus')
        # self.acq_db= self.config.get('acq_db')

        #rama
        self.result_local_path_PWC = self.config.get('result_local_path_PWC')
        self.destination_connection_PWC = self.config.get('destination_connection_PWC')
        self.destination_path_PWC = self.config.get('destination_path_PWC')
        self.destination_connection_type_PWC = self.config.get('destination_connection_type_PWC')

        #roy
        self.destination_schema = self.config.get('destination_schema')
        self.destination_table = self.config.get('destination_table')
        self.destination_table_2 = self.config.get('destination_table_2')
        self.destination_table_dev_test = self.config.get('destination_table_dev_test')

        #query related below
        self.kwares_db_source = self.config.get('kwares_db_source')
        logging.info(f"config: kwares_db_source={self.kwares_db_source}")

    #custom function for to be called by dags function start
    def process_file_mask(self, outgoing_file_name = ""):
        list_file_mask = []
        context = context = get_current_context()
        date = context["execution_date"]
        logging.info('Execution/1 : ' + str(date))
        date = date + datetime.timedelta(hours = 9)
        logging.info('Execution/2 : ' + str(date))
        date = date + datetime.timedelta(days=self.fetch_date)
        date_string = date.strftime(self.format_date)
        logging.info('Execution/Logical Date : ' + date_string)

        file_mask = self.file_name_mask
        if len(outgoing_file_name) != 0:
            file_mask = outgoing_file_name

        file_name_mask = str(file_mask.replace('.dsj', date_string))
        list_file_mask = file_name_mask.split("|")
        return list_file_mask
    #custom function for to be called by dags function end

    def check_chk_exists(self):
        try:
            #init data
            flag = True
            # context = context = get_current_context()
            # date = context["execution_date"]
            # date = date + datetime.timedelta(days=self.fetch_date)
            # date_string = date.strftime(self.format_date)

            # file_mask = self.file_name_mask.replace('.dsj', date_string)
            # if len(self.file_name_mask_outgoing) != 0:
            #     file_mask = self.file_name_mask_outgoing.replace('.dsj', date_string)
            # file_mask = file_mask + ".chk"
            # logging.info('File Mask : ' + file_mask)
            list_file_mask = []
            list_file_mask = self.process_file_mask(self.file_name_mask_outgoing) #kalo masih string kosong otomatis ke self.file_name_mask

            if self.destination_connection_type == "FTP":
                logging.info('Connection Type : ' + self.destination_connection_type)
                destination_hook = FTPHook(ftp_conn_id=self.destination_connection)

                for f in list_file_mask:
                    file_mask = f + ".chk"
                    logging.info('chk file : ' + self.destination_path + file_mask)
                    files = sorted(destination_hook.list_directory(self.destination_path))
                    for file in files:
                        if fnmatch.fnmatch(file, file_mask):
                            logging.info('scheduler already run for today')
                            flag = False
                            break

                destination_hook.close_conn()
            elif self.destination_connection_type == "SFTP":
                logging.info('Connection Type : ' + self.destination_connection_type)
                destination_hook = SFTPHook(ssh_conn_id=self.destination_connection)

                for f in list_file_mask:
                    file_mask = f + ".chk"
                    logging.info('chk file : ' + self.destination_path + file_mask)
                    for file in destination_hook.list_directory(self.destination_path):
                        if fnmatch.fnmatch(file, file_mask):
                            logging.info('scheduler already run for today')
                            flag = False
                            break

                destination_hook.close_conn()
            else:
                logging.info('Invalid Connection Type : ' + self.destination_connection_type)
            return flag
        except Exception as e:
            logging.error(f"Error check_chk_exists: error: {e}")
            return False

    def get_files(self):

        try:
            logging.info('---------- START get_files ----------')
            logging.info('source_connection= %s', self.source_connection)
            logging.info('local_path= %s', self.local_path)
            logging.info('source_path=%s', self.source_path)

            context = get_current_context()
            date = context["execution_date"]
            date = date + datetime.timedelta(days=self.fetch_date)
            date_string = date.strftime("%y%m%d")
            logging.info('date_string: %s', date_string)
            #init data
            if not os.path.exists(self.local_path+date_string[0:2]+'/'+date_string[2:4]+'/'+date_string[4:6]+'/'): #create folder sesuai dengan tanggal file
                os.makedirs(self.local_path+date_string[0:2]+'/'+date_string[2:4]+'/'+date_string[4:6]+'/')
            logging.info('check path :'+self.local_path+date_string[0:2]+'/'+date_string[2:4]+'/'+date_string[4:6]+'/')
            list_file_mask = []
            list_file_mask = self.process_file_mask()

            if self.source_connection_type == "FTP":
                logging.info('Connection Type : ' + self.source_connection_type)
                source_hook = FTPHook(ftp_conn_id=self.source_connection)
                files = sorted(source_hook.list_directory(self.source_path))
                for file in files:
                    # if (source_hook.isfile(self.source_path + file)):
                    basename_file = os.path.basename(file)
                    for file_mask in list_file_mask:
                        logging.info('Files: ' + basename_file + ' Mask: ' + file_mask)
                        if fnmatch.fnmatch(basename_file, file_mask):
                            logging.info('Downloading Files: ' + basename_file)
                            source_hook.retrieve_file(file, self.local_path+date_string[0:2]+'/'+date_string[2:4]+'/'+date_string[4:6]+'/'+ basename_file)
                            logging.info('Berhasil Download ' + basename_file)
                        elif FileNotFoundError:
                            logging.info('File ' + basename_file + ' Not Found')
                source_hook.close_conn()
            elif self.source_connection_type == "SFTP":
                logging.info('Connection Type : ' + self.source_connection_type)
                source_hook = SFTPHook(ssh_conn_id=self.source_connection)
                files = sorted(source_hook.list_directory(self.source_path))
                for file in files:
                    if (source_hook.isfile(self.source_path + file)):
                        for file_mask in list_file_mask:
                            if fnmatch.fnmatch(file, file_mask):
                                logging.info('Downloading Files: ' + file)
                                source_hook.retrieve_file(self.source_path + file, self.local_path+date_string[0:2]+'/'+date_string[2:4]+'/'+date_string[4:6]+'/'+ file)
                                logging.info('Berhasil Download ' + file)
                            elif FileNotFoundError:
                                logging.info('File ' + file + ' Not Found')
                source_hook.close_conn()
            else:
                logging.info('Invalid Connection Type ! ' + self.source_connection_type)
        except Exception as e:
            logging.error(f'Error get_file error : {e}')

    def extract_all_zip_files(self):
        try:
            logging.info('========== START ==============')
            context = get_current_context()
            date = context["execution_date"]
            date = date + datetime.timedelta(days=self.fetch_date)
            date_string = date.strftime("%y%m%d")

            logging.info('execution date:%s', date)
            logging.info('date_string=%s', date_string)
            logging.info('local_path=%s', self.local_path)

            if os.path.exists(self.local_path+date_string[0:2]+'/'+date_string[2:4]+'/'+date_string[4:6]+'/'):
                logging.info('folder found: %s', self.local_path + date_string)

                for y in glob.glob(self.local_path+date_string[0:2]+'/'+date_string[2:4]+'/'+date_string[4:6]+'/' + "*.zip"):
                    logging.info('ListDirectory: %s' + y)
                    if ".zip" in y:
                        logging.info('Extracting: %s' + y)
                        with zipfile.ZipFile(y, 'r') as zip_ref:
                            zip_ref.extractall(self.local_path+date_string[0:2]+'/'+date_string[2:4]+'/'+date_string[4:6]+'/')
                    logging.info('done extract: %s', y)
            else:
                logging.info('Path ' + self.local_path+date_string[0:2]+'/'+date_string[2:4]+'/'+date_string[4:6]+'/' + ' Tidak ditemukan')
        except Exception as e:
            logging.error(f"Error extract_all_zip_files: error: {e}")

    def split_multiple_files(self):
        logging.info('source_path: %s', self.source_path)
        logging.info('pwc_path: %s', self.target_path)
        logging.info('local_path: %s', self.local_path)
        logging.info('mti_path: %s', self.destination_path)
        logging.info('result_local_path_PWC: %s', self.result_local_path_PWC)

        try:
            context = get_current_context()
            date = context["execution_date"]

            if date is None:
                raise ValueError('date is none')

            date = date + datetime.timedelta(days=self.fetch_date)
            date_string = date.strftime("%y%m%d")

            dateformat = date_string[0:2]+'/'+date_string[2:4]+'/'+date_string[4:6]+'/'

            logging.info('date_format:%s', dateformat)

            local_path = self.local_path
            if not os.path.isdir(local_path):
                logging.warning(
                    "skip split_multiple_files: local path=%s tidak di temukan",
                    local_path
                )
                return

            list_file_mask = self.process_file_mask(
                self.file_name_mask_outgoing
            )

            local_files = os.listdir(local_path+dateformat)
            logging.info('local_files:%s', local_files)

            files_to_send = [
                filename
                for filename in local_files
                if any(fnmatch.fnmatch(filename, mask)
                    for mask in list_file_mask)
            ]

            logging.info('list_file_mask: %s', list_file_mask)
            logging.info('file_to_send: %s', files_to_send)

            if not files_to_send:
                logging.info('tidak ada file yang cocok: %s', local_files)
                return

            #conn id MTI & PWC
            destination_hook_mti = FTPHook(ftp_conn_id=self.destination_connection)
            destination_hook_pwc = SFTPHook(ssh_conn_id=self.destination_connection_PWC_POST)

            if self.is_split:
                logging.info('===============SPLIT & SEND FILE START===============')
                self.kwares_db_source = getattr(self, 'kwares_db_source', None)

                split_class = SplitClass(self.kwares_db_source)

                timestamp = split_class.get_timestamp_POST()

                #buat folder sebelum action
                result_local_path_PWC = os.path.join(self.result_local_path_PWC, dateformat)
                result_local_path = os.path.join(self.result_local_path, dateformat)
                os.makedirs(result_local_path, exist_ok=True)
                os.makedirs(result_local_path_PWC,exist_ok=True)

                logging.info('result_local_path:%s', result_local_path)
                logging.info('result_local_path_pwc:%s', result_local_path_PWC)

                for filename in files_to_send:
                    logging.info('=============== SENDING FILE START =================')
                    logging.info('processing file: %s', filename)

                    #file hasil extract
                    source_file = os.path.join(local_path, dateformat, filename)
                    logging.info('input_file: %s', source_file)

                    #copy ke folder pwc
                    mti_source_file = os.path.join(result_local_path, filename)
                    pwc_source_file = os.path.join(result_local_path_PWC, filename)
                    logging.info('pwc_source_file: %s', pwc_source_file)
                    logging.info('mti_source_file: %s', mti_source_file)

                    #copy file sebelum di split
                    shutil.copy(source_file, pwc_source_file)
                    logging.info('Copy file pwc success')
                    logging.info('From: %s', source_file)
                    logging.info('To: %s', pwc_source_file)

                    #copy file sebelum di split
                    shutil.copy(source_file, mti_source_file)
                    logging.info('Copy file mti success')
                    logging.info('From: %s', source_file)
                    logging.info('To: %s', mti_source_file)

                    #cek file exist folder
                    logging.info('pwc file exist: %s', os.path.isfile(mti_source_file))
                    logging.info('mti file exist: %s', os.path.isfile(pwc_source_file))

                    #split file pwc
                    logging.info('=============== SPLIT START ==============')
                    logging.info('start split MTI: %s', pwc_source_file)
                    logging.info('start split PWC: %s', mti_source_file)
                    # Kode lama hanya memproses data Rintis tanpa mengambil Way4:
                    # generated_posting_files = split_class.split_rintis_qr_recon(
                    #     str_file_name=source_file,
                    #     str_result_name=mti_source_file,
                    #     str_result_pwc=pwc_source_file,
                    # )
                    # Keterangan: Way4 harus diambil dan dipetakan sebelum generator
                    # dipanggil agar Rintis dan Way4 masuk ke POSTFLIN yang sama.
                    period = date.strftime("%Y%m%d")
                    way4_records = self.get_way4_data(
                        period=period,
                        save_result_file=False,
                    )
                    way4_posting_records = split_class.map_way4_to_posting_records(
                        way4_records
                    )
                    logging.info(
                        'Way4 records ready to combine: %s',
                        len(way4_posting_records),
                    )

                    generated_posting_files = split_class.split_rintis_qr_recon(
                        str_file_name=source_file,
                        str_result_name=mti_source_file,
                        str_result_pwc=pwc_source_file,
                        way4_posting_records=way4_posting_records,
                    )
                    # Kode lama:
                    # split_class.split_rintis_qr_recon(str_file_name=source_file, str_result_name=mti_source_file, str_result_pwc=pwc_source_file)
                    # mapped_rintis = split_class.split_rintis_qr_recon(str_file_name=source_file, str_result_name=mti_source_file, str_result_pwc=pwc_source_file)

                    logging.info('finish split MTI: %s', mti_source_file)
                    logging.info('finish split PWC: %s', pwc_source_file)
                    logging.info('MTI file exist: %s', os.path.isfile(mti_source_file))
                    logging.info('PWC file exist: %s', os.path.isfile(pwc_source_file))

                    logging.info('============= SPLIT END ==============')
                    #validasi file split
                    if not os.path.isfile(source_file):
                        raise FileNotFoundError(f'file split not found')

                    QR_RECON_files = glob.glob(os.path.join(self.result_local_path_PWC, "QR_RECON*"))
                    for qr_file in QR_RECON_files:
                        os.remove(qr_file)
                        logging.info('Deleted raw QR_RECON file: %s', qr_file)
                    logging.info('============= SEND START ==============')

                    #create chk file mti
                    logging.info('Creating .chk file mti')

                    split_name_mti = f"POSTFLIN_{timestamp}"
                    remote_file_mti = f"POSTFLIN_{timestamp}.txt"

                    chk_name_mti = split_name_mti + '.chk'
                    chk_file_mti = os.path.join(result_local_path,chk_name_mti)
                    logging.info('chk_name_mti:%s', chk_name_mti)
                    logging.info('chk_file_mti:%s', chk_file_mti)
                    with open(chk_file_mti, 'w') as f:
                        pass

                    remote_file_mti = os.path.join(self.destination_path, chk_name_mti).replace("\\", "/")
                    destination_hook_mti.store_file(
                        remote_file_mti,
                        chk_file_mti,
                    )
                    logging.info(
                        'sukses upload chk hasil split Rintis %s -> %s',
                        chk_file_mti,
                        remote_file_mti,
                    )
                    #COMBINED FILE RINTIS & WAY4
                    # way4_record = self.get_way4_data()
                    # mapped_way4 = split_class.map_way4_query_result(way4_record)
                    # combined_pwc_file = split_class.combine_mapped_posting_result(mapped_rintis=mapped_rintis, mapped_way4=mapped_way4, output_directory
                    print('========= BEFORE PWC STORE =========', flush=True)
                    # Keterangan: kirim setiap POSTFLIN_*.txt yang benar-benar dibuat
                    # oleh create_posting_file(), menggunakan nama file hasil generate.
                    for generated_posting_file in generated_posting_files:
                        remote_file_pwc = os.path.join(
                            self.target_path,
                            os.path.basename(generated_posting_file),
                        ).replace("\\", "/")
                        print('remote_file_pwc=', remote_file_pwc, flush=True)
                        destination_hook_pwc.store_file(
                            remote_file_pwc,
                            generated_posting_file,
                        )
                        logging.info(
                            'sukses upload hasil generate %s -> %s',
                            generated_posting_file,
                            remote_file_pwc,
                        )

                    if not generated_posting_files:
                        logging.warning(
                            'tidak ada POSTFLIN hasil generate untuk dikirim ke PWC'
                        )

                    # Kode lama: yang dikirim adalah pwc_source_file berformat
                    # RH/DH/RT dengan nama remote .chk, bukan hasil generator.
                    # destination_hook_pwc.store_file(remote_file_pwc, pwc_source_file)
                    # destination_hook_pwc.store_file(remote_file_pwc, combined_pwc_file)

                    destination_hook_pwc.close_conn()
                    print('========= AFTER PWC STORE =========', flush=True)

                    logging.info('============= SEND END ==============')

                    logging.info('sukses upload %s -> %s', remote_file_mti+chk_name_mti, remote_file_mti)
                    # Kode lama: logging.info('sukses upload %s -> %s', pwc_source_file, remote_file_pwc)

                    logging.info('============= SEND & SPLIT END ==============')
            else:
                logging.info('Path MTI', self.result_local_path, 'Not found')
                logging.info('Path PWC', self.result_local_path_PWC, 'Not found')
                logging.info('============= END NO SPLIT ==============')
                # logging.info('============= SENDING UNPROCESSED FILE ==============')
                # for filename in files_to_send:
                #     local_file_mti = os.path.join(result_local_path, filename)
                #     local_file_pwc = os.path.join(result_local_path_PWC, filename)
                #
                #     logging.info('local file mti unprocessed:%s', local_file_mti)
                #     logging.info('local file pwc unprocessed:%s', local_file_mti)
                #
                #     #send to mti
                #     destination_hook_mti.store_file(remote_file_mti, local_file_mti)
                #     destination_hook_mti.close_conn()
                #
                #     #send to pwc
                #     destination_hook_pwc.store_file(remote_file_pwc, local_file_pwc)
                #     destination_hook_pwc.close_conn()
        except Exception as e:
            logging.exception(e)
            raise

    # Kode lama sebelum implementasi dari foto:
    # def get_way4_data(self):
    #     logging.info('========== START GET WAY4 DATA ==========')
    #     split_class = SplitClass()
    #     records = split_class.get_data_fetch_one()
    #     logging.info('========== END GET WAY4 DATA: %s ==========', records)
    #     return records
    # Kode lama hasil salinan foto:
    # def get_way4_data(self, period=None):
    # Keterangan: parameter save_result_file ditambahkan agar proses combine dapat
    # mengambil record tanpa membuat file Way4 terpisah sebelum POSTFLIN digabung.
    def get_way4_data(self, period=None, save_result_file=True):
        con_acq = None
        try:
            split_class = SplitClass(self.kwares_db_source)
            timestamp = split_class.get_timestamp_POST()
            if period is None:
                if len(timestamp) < 8 or not timestamp[:8].isdigit():
                    raise ValueError(
                        f"Format timestamp POST tidak valid: {timestamp!r}"
                    )
                period = timestamp[:8]

            get_con_acq = split_class.get_data_fetch_one()
            con_acq = tuple(
                sorted(
                    {
                        str(row[0]).strip()
                        for row in get_con_acq
                        if row and row[0] is not None and str(row[0]).strip()
                    }
                )
            )

            if not con_acq:
                logging.warning('Tidak ada connection_acq yang bisa diproses')
                return []

            logging.info('get_con_acq:%s', con_acq)
            logging.info('period:%s', period)

            con_acq_params = {
                f"con_acq_{index}": value
                for index, value in enumerate(con_acq)
            }
            con_acq_placeholder = ", ".join(
                f"%(con_acq_{index})s" for index in range(len(con_acq))
            )

            sql = """
                SELECT
                    institutionbranch_acq,
                    sourceregnum,
                    pan,
                    period,
                    periodtime,
                    stan,
                    rrn,
                    institution_acq,
                    transactiontype,
                    settlementdate,
                    settlementcurrency,
                    reconamount,
                    transactioncurrency,
                    transactionchargeamount,
                    merchantid,
                    auth_code,
                    rc,
                    reversalseq
                FROM sw_replicate.on_doc_transaction
                WHERE period = %(period)s
                  AND connection_acq IN ({con_acq})
                ORDER BY period DESC
                LIMIT 10
            """.format(con_acq=con_acq_placeholder)

            logging.info('============ START GET DATA WAY4 ============')
            logging.info('period: %s', period)
            logging.info('con_acq: %s', con_acq)
            logging.info('sql: %s', sql)

            way4 = Way4DB()
            logging.info('connection db way4')

            records = way4.get_data(
                sql=sql,
                params={"period": period, **con_acq_params},
            )

            if records is None:
                logging.warning(
                    'way4 get data return None | period=%s | con_acq=%s',
                    period,
                    con_acq,
                )
                records = []

            if records and save_result_file:
                os.makedirs(self.result_local_way4, exist_ok=True)
                spek_name_pwc = f"POSTFLIN_{timestamp}.txt"
                result_file = os.path.join(self.result_local_way4, spek_name_pwc)
                logging.info('Save way4 result to file:%s', result_file)

                with open(result_file, "w") as file:
                    for row in records:
                        file.write(
                            "|".join(
                                "" if value is None else str(value)
                                for value in row
                            )
                        )
                        file.write("\n")

                logging.info(
                    'way4 result file created:%s | total records:%s',
                    result_file,
                    len(records),
                )

            return records
        except Exception as e:
            logging.exception(
                'Gagal mengambil data WAY4 | period=%s | con_acq=%s | error: %s',
                period,
                con_acq,
                e,
            )
            raise


class SplitClass():
    def __init__(self, kwares_db_source):
        self.kwares_db_source = kwares_db_source
        logging.info(f"config: kwares_db_source={self.kwares_db_source}")
        

    def split_rintis_qr_recon(
        self,
        str_file_name,
        str_result_name,
        str_result_pwc,
        way4_posting_records=None,
    ):
        try:
            logging.info('========== SPLIT PROCESSED START QR RECON ==========')

            #open file source path
            file_unprocessed = open(str_file_name, 'r')

            #output MTI
            file_processed = open(str_result_name, 'w')

            #output PWC
            file_processed_PWC = open(str_result_pwc, 'w')

            merchants = self.get_data_merchant_pwc_qr_acceptor()
            merchants_pwcs = set(str(row[0]).strip() for row in merchants)
            # merchants_pwcs = {str(row[0]).strip() for row in merchants if row and row[0] is not None}

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
                if line.startswith("RH"):
                    logging.info('===== START RH RECORD =====')
                    hd = ReconHeader.parse_header(line)
                    file_processed.write(line)
                    file_processed_PWC.write(hd.to_header_line() + "\n")
                    logging.info('===== END RH RECORD =====')
                elif line.startswith("DH"):
                    logging.info('=== START DH RECORD ===')
                    rec = ReconRecordData.parse_recon_data_line(line)
                    mpan = str(rec.merchant_pan).strip()

                    # logging.info('REC: %s', rec)
                    # logging.info('BANK: %s', rec.acquiring_bank_code)
                    # logging.info('MPAN hasil mapping rintis = [%s]', mpan)
                    # logging.info('Merchant PWC = %s', merchants_pwcs)

                    logging.info('=== END DH RECORD ===')
                    if mpan in merchants_pwcs:
                        logging.info('========== START CREATE FILE SPLIT ==========')
                        logging.info('Merchants is PWC')
                        mid_records = self.get_merchant_by_acceptor_point(mpan)

                        if not mid_records:
                            logging.info('MID not found for MPAN [%s]', mpan)
                            continue

                        mid = mid_records[0][0]
                        logging.info('MID record:%s', mid_records)
                        mid = str(mid).strip()

                        logging.info('[QR_RECON] MID selected | mpan:%s | type:%s | lenght:%s', mpan,mid,type(mid).__name__,len(str(mid)))

                        filename_datetime = self.get_timestamp_POST()

                        if posting_filename is None:
                            posting_filename = f"POSTFLIN_{filename_datetime}.txt"
                        filename = posting_filename

                        logging.info('[QR_RECON] Calling create_posting_file | filename:%s | mpan:%s | mid:%s | target_split:%s ', filename,mpan,mid,str_result_pwc)

                        pwc_posting_records.append((rec, mid))
                        file_processed_PWC.write(rec.to_line() + "\n")
                        row_count_pwc += 1
                        # record_pwc += int(rec.dispute_amount)
                        logging.info('rec_to_line:%s', rec.to_line())
                        logging.info('filename_split:%s', filename)
                        logging.info('=== END CREATE FILE SPLIT ===')

                    else:
                        logging.info('merchant MTI')
                        file_processed.write(line)
                        row_count += 1
                        # record += int(rec.dispute_amount)

                elif line.startswith("RT"):

                    file_processed.write(line[:28] + str(row_count)+'|'+str(record))
                    file_processed_PWC.write(hd.to_header_line()+'|'+str(record_pwc))

                    pwc_posting_records.extend(way4_posting_records)
                    if posting_filename is None and pwc_posting_records:
                        posting_filename = (
                            f"POSTFLIN_{self.get_timestamp_POST()}.txt"
                        )
                    if pwc_posting_records:
                        generated_posting_file = self.create_posting_batch_file(
                            posting_filename,
                            pwc_posting_records,
                            target_split=str_result_pwc,
                            header=hd,
                        )
                        generated_posting_files.append(generated_posting_file)
                    logging.info('==========SPLIT PROCESSED END QR RECO ==========')
            return generated_posting_files
        except Exception:
            logging.info('Error split rintis RAW')
            raise

    def get_data_merchant_pwc_qr_acceptor(self):
        logging.info('START Get Data merchant PWC')
        logging.info(f"get_data: kwares_db_source={self.kwares_db_source}")
        try:
            pwcDB = PwcDB()
            merchant_query = "SELECT AP_ON_ID_10 FROM MER_ACCEPTOR_POINT"
            logging.info('get data merchant pwc')
            logging.info('executing query: %s', merchant_query)
            logging.info('connection db PWC')
            records = pwcDB.get_data_pwc(sql=merchant_query)

            if records is None:
                logging.warning('pwc get data return None')
                records = []

            logging.info('final result ap_on_id: %s', records)
            return records

        except Exception as e:
            logging.exception("Gagal mengambil data pwc")
            raise

    def get_merchant_by_acceptor_point(self, mpan: str):
        try:
            sql = """
                SELECT MER_ACCEPTOR_POINT_ID FROM MER_ACCEPTOR_POINT WHERE TRIM(AP_ON_ID_10) = TRIM(:mpan)
            """
            mpan = str(mpan).strip()

            logging.info('==== START GET DATA MID FROM MPAN ====')
            logging.info('sql:%s', sql)
            logging.info('mpan acceptor:%s', mpan)

            pwc = PwcDB()

            records = pwc.get_fetch_one(sql=sql, params={"mpan": mpan})

            if records is None:
                logging.warning('pwc get data return None')
                records = []
            logging.info('records result acceptor point: %s', records)
            return records
        except Exception as e:
            logging.exception('Gagal ambil data MID dari PWC')
            raise

    def create_posting_batch_file(self, filename, posting_records, target_split: str, header=None):
        """Membuat satu POSTFLIN dan mengelompokkan transaksi per merchant/batch."""
        if not filename or not filename.endswith(".txt"):
            raise ValueError(f"Invalid posting filename: {filename!r}")
        if ".." in filename or "/" in filename or "\\" in filename:
            raise ValueError("filename cannot contain a path")

        full_path = os.path.join(os.path.dirname(target_split), filename)

        grouped_records = {}
        for rec, mid in posting_records:
            normalized_mid = str(mid).strip()
            group_key = (
                normalized_mid,
                str(rec.merchant_pan).strip(),
                str(rec.terminal_id).strip(),
            )
            grouped_records.setdefault(group_key, []).append(rec)

        sequence_number = 1
        transaction_sequence = 0
        voucher_sequence = 0
        posting_lines = [self.generate_hr_record(header).rstrip("\n")]

        for (mid, _mpan, _terminal_id), records in grouped_records.items():
            first_rec = records[0]
            transaction_sequence = 0
            sequence_number += 1
            posting_lines.append(
                self.generate_hs_record(first_rec, mid, sequence_number).rstrip("\n")
            )

            for rec in records:
                transaction_sequence += 1
                voucher_sequence += 1
                sequence_number += 1
                posting_lines.append(
                    self.generate_dt_record(
                        rec, sequence_number, transaction_sequence, voucher_sequence
                    ).rstrip("\n")
                )
                sequence_number += 1
                posting_lines.append(
                    self.generate_oa_record(
                        rec, sequence_number, voucher_sequence
                    ).rstrip("\n")
                )

            sequence_number += 1
            posting_lines.append(
                self.generate_ts_record(
                    first_rec,
                    mid,
                    sequence_number,
                    group_records=records,
                ).rstrip("\n")
            )

        sequence_number += 1
        posting_lines.append(self.generate_tr_record(sequence_number, posting_lines))
        full_containt = "\n".join(posting_lines)

        expected_lengths = {"HR": 47, "HS": 89, "DT": 147, "OA": 184, "TS": 133, "TR": 74}
        for line in posting_lines:
            expected_length = expected_lengths.get(line[:2])
            if expected_length is None or len(line) != expected_length:
                raise ValueError(
                    f"Invalid {line[:2]} record length: {len(line)}, expected {expected_length}"
                )

        with open(full_path, "w", encoding="utf-8") as generated_file:
            generated_file.write(full_containt)

        logging.info(
            "Posting batch generated: %s | groups=%s | transactions=%s | records=%s",
            full_path,
            len(grouped_records),
            voucher_sequence,
            len(posting_lines),
        )
        return full_path

    def generate_hr_record(self, header=None):
        record_type = "HR"

        last_sequence_number = 0
        this_sequence = last_sequence_number + 1

        sequence_name = self.pad_with_spaces(self.to_padded_number(8,this_sequence), 8)

        bank_code_id = "ID7339"

        institution_ref = self.pad_with_spaces(bank_code_id, 6)

        today_yymmdd = self.pad_with_spaces(self.get_today_yymmdd(), 6)
        ptidn = self.pad_with_spaces(self.to_padded_number(6,this_sequence), 6)
        file_ref_value = f"PTIN{ptidn}{today_yymmdd}"

        file_ref = self.pad_with_spaces(file_ref_value, 16)
        recon_date = str(header.recon_file_date).strip() if header else ""
        processDate = (
            recon_date + datetime.datetime.now().strftime("%H%M%S")
            if len(recon_date) == 8
            else self.get_timestamp_POST()
        )
        tokenization_indicator = "C"

        # Merging String
        result = record_type + sequence_name + institution_ref + file_ref + processDate + tokenization_indicator

        if len(result) != 47:
            raise ValueError(f"invalid HR RECORD length:{len(result)}, expected 47")
        return result + "\n"

    # Kode lama: def generate_tr_record(self):
    def generate_tr_record(self, sequence_number=1, posting_lines=None):
        record_type = "TR"

        last_sequence_number = 0
        this_sequence = last_sequence_number + 1

        sequence_in_file = self.to_padded_number(8, sequence_number)

        institution_identification = self.pad_with_spaces("ID7339", 6)
        file_sender = self.pad_with_spaces("360002", 6)

        posting_lines = posting_lines or []
        dt_lines = [line for line in posting_lines if line.startswith("DT")]
        debit_lines = [line for line in dt_lines if len(line) >= 113 and line[112] == "D"]
        credit_lines = [line for line in dt_lines if len(line) >= 113 and line[112] == "C"]
        count_debit = self.to_padded_number(8, len(debit_lines))
        count_credit = self.to_padded_number(8, len(credit_lines))
        amount_debit_value = sum(int(line[94:112].strip() or "0") for line in debit_lines)
        amount_credit_value = sum(int(line[94:112].strip() or "0") for line in credit_lines)
        amount_debit = str(amount_debit_value).zfill(18)
        amount_credit = str(amount_credit_value).zfill(18)

        result = record_type + sequence_in_file + institution_identification + file_sender + count_debit + count_credit + amount_credit + amount_debit

        if len(result) != 74:
            raise ValueError(f"invalid TR RECORD length:{len(result)}, expected 74")

        return result

    # Kode lama: def generate_hs_record(self, rec: ReconRecordData, mid: str):
    def generate_hs_record(self, rec: ReconRecordData, mid: str, sequence_number=1):
        logging.info('========== GENERATE HS RECORD ==============')
        logging.info('MID:%s', mid)
        logging.info('REC:%s', rec)
        logging.info('Terminal ID :%s', rec.terminal_id)

        record_type = "HS"

        last_sequence = 0
        new_sequence = last_sequence + 1

        last_batch = 0
        new_batch = last_batch + 1

        # Kode lama: record_sequence = self.to_padded_number(8, new_sequence)
        record_sequence = self.to_padded_number(8, sequence_number)
        merchant_number = self.pad_with_spaces(mid[:15], 15)

        outlet_value = str(getattr(rec, "outlet_number", mid))
        outlet_number = self.pad_with_spaces(outlet_value[:15], 15)
        terminal_id = self.pad_with_spaces(rec.terminal_id[:15], 15)
        batch_number = self.to_padded_number(8, new_batch)

        capture_date = str(
            getattr(rec, "batch_capture_date", self.get_today_yyyymmdd())
        )
        batch_capture_date = self.pad_with_spaces(capture_date[:8], 8)
        batch_datetime = self.pad_with_spaces(
            f"{rec.transaction_date}{rec.transaction_time}", 14
        )

        batch_currency_value = str(
            getattr(rec, "batch_currency", rec.transaction_amount_currency)
        )
        batch_currency = self.pad_with_spaces(batch_currency_value[:3], 3)
        batch_type = "P"

        # Merging String
        result = record_type + record_sequence + merchant_number + outlet_number + terminal_id + batch_number + batch_capture_date + batch_datetime + batch_currency + batch_type
        logging.info('HS final result:[%s]', result)
        logging.info('HS LENGHT = %s', len(result))

        if len(result)!=89:
            raise ValueError(f"invalid HS RECORD lenght:{len(result)},expected 89")

        return result + "\n"

    def generate_dt_record(
        self,
        rec: ReconRecordData,
        sequence_number=1,
        transaction_sequence=1,
        voucher_sequence=None,
    ):
        record_type = "DT"

        last_sequence = 0
        new_sequence = last_sequence + 1

        last_trx_batch = 0
        new_trx_batch = last_trx_batch + 1

        voucher_number_sequence = (
            transaction_sequence if voucher_sequence is None else voucher_sequence
        )
        voucher_generated = self.to_padded_number(8, voucher_number_sequence)

        record_sequence_in_file = self.to_padded_number(8, sequence_number)
        transaction_sequence_in_batch = self.to_padded_number(7, transaction_sequence)
        service_type = "0"
        voucher_number = self.pad_with_spaces(voucher_generated[:8], 8)
        card_number = self.pad_with_spaces(rec.customer_pan[:22], 22)
        expiry_date = self.generate_spaces_int(6)
        processing_date = self.pad_with_spaces(rec.processing_code[:6], 6)
        reversal_flag = str(getattr(rec, "reversal_flag", "N"))[:1]
        authorization_flag = "A"
        post_date = self.pad_with_spaces("100001154110"[:12], 12)
        # Kode lama: post_entry_mode = self.pad_with_spaces("A2", 4)
        post_entry_mode = self.pad_with_spaces("012"[:4], 4)
        post_condition_code = self.pad_with_spaces("00"[:2], 2)
        transaction_datetime = self.pad_with_spaces(f"{rec.transaction_date}{rec.transaction_time}", 14)
        transaction_amount = self.to_fixed_numeric(rec.transaction_amount[:18], 18)
        transaction_sign = "D" if rec.processing_code[:2] in {"20", "29"} else "C"
        transaction_currency = self.pad_with_spaces(rec.transaction_amount_currency[:3], 3)
        currency_exponent = "1"
        reversal_reason_code = self.generate_spaces_int(2)
        replacement_amounts = self.generate_spaces_int(18)
        authorization_code = self.pad_with_spaces(rec.approval_code[:6], 6)
        service_code = self.generate_spaces_int(3)
        single_message_indicator = "Y"

        # Merging String
        result = record_type + record_sequence_in_file + transaction_sequence_in_batch + service_type + voucher_number + card_number + expiry_date + processing_date + reversal_flag + authorization_flag + post_date + post_entry_mode + post_condition_code + transaction_datetime + transaction_amount + transaction_sign + transaction_currency + currency_exponent + reversal_reason_code + replacement_amounts + authorization_code + service_code + single_message_indicator
        if len(result) != 147:
            raise ValueError(f"invalid DT RECORD length:{len(result)}, expected 147")
        return result + "\n"

    def generate_oa_record(self, rec: ReconRecordData, sequence_number=1, voucher_sequence=1):
        record_type = "OA"

        last_sequence = 0
        new_sequence = last_sequence + 1

        # Kode lama: sequence = self.to_padded_number(8, new_sequence)
        sequence = self.to_padded_number(8, sequence_number)

        record_sequence = sequence
        voucher_number = self.to_padded_number(8, voucher_sequence)
        tip_amount = self.generate_spaces_int(18)
        cashback_amount = self.generate_spaces_int(18)
        fee = self.to_fixed_numeric(rec.convenience_fee[:18], 18)
        surcharge_fee = self.generate_spaces_int(18)
        billing_amount = self.to_fixed_numeric(rec.transaction_amount[:18], 18)
        billing_currency = self.pad_with_spaces(rec.transaction_amount_currency[:3], 3)
        conversion_rate = self.generate_spaces_int(12)
        rate_exponent = self.generate_spaces_int(2)
        rate_date = self.generate_spaces_int(14)
        reversed_for_future_use = self.generate_spaces_int(9)
        external_ref_id = self.pad_with_spaces(rec.invoice_data[:23], 23)
        dcc_indicator = self.generate_spaces_int(1)
        reversed_for_future_use2 = self.pad_with_spaces(rec.retrieval_reference_number[:12], 12)
        result = record_type + record_sequence + voucher_number + tip_amount + cashback_amount + fee + surcharge_fee + billing_amount + billing_currency + conversion_rate + rate_exponent + rate_date + reversed_for_future_use + external_ref_id + dcc_indicator + reversed_for_future_use2

        if len(result) != 184:
            raise ValueError(f"invalid OA RECORD length:{len(result)}, expected 184")
        return result + "\n"

    def generate_ts_record(
        self, rec: ReconRecordData, mid: str, sequence_number=1, group_records=None
    ):
        record_type = "TS"

        last_sequence = 0
        new_sequence = last_sequence + 1

        last_batch = 0
        new_batch = last_batch + 1

        sequence_in_file = self.to_padded_number(8, sequence_number)
        merchant_number = self.pad_with_spaces(mid[:15], 15)
        outlet_value = str(getattr(rec, "outlet_number", mid))
        outlet_number = self.pad_with_spaces(outlet_value[:15], 15)
        terminal_id = self.pad_with_spaces(rec.terminal_id[:15], 15)
        batch_number = self.to_padded_number(8, new_batch)
        batch_capture_date = self.pad_with_spaces(self.get_today_yyyymmdd(), 8)
        batch_datetime = self.pad_with_spaces(
            f"{rec.transaction_date}{rec.transaction_time}", 14
        )
        records = group_records or [rec]
        debit_records = [r for r in records if r.processing_code[:2] in {"20", "29"}]
        credit_records = [r for r in records if r.processing_code[:2] not in {"20", "29"}]
        record_count_debit = self.to_padded_number(6, len(debit_records))
        net_amount_debit = self.to_fixed_numeric(
            sum(int(r.transaction_amount) for r in debit_records), 18
        )
        record_count_credit = self.to_padded_number(6, len(credit_records))
        net_amount_credit = self.to_fixed_numeric(
            sum(int(r.transaction_amount) for r in credit_records), 18
        )

        result = record_type + sequence_in_file + merchant_number + outlet_number + terminal_id + batch_number + batch_capture_date + batch_datetime + record_count_debit + net_amount_debit + record_count_credit + net_amount_credit
        if len(result) != 133:
            raise ValueError(f"invalid TS RECORD length:{len(result)}, expected 133")
        return result + "\n"

    def get_timestamp_POST(self):
        try:
            # Generate current datetime and format it
            return datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        except Exception as e:
            # Fallback handling (very rare for datetime operations)
            raise RuntimeError(f"Failed to generate timestamp: {e}")

    def get_today_yymmdd(self):
        try:
            today = datetime.datetime.today()  # Get current local date and time
            return today.strftime("%y%m%d")  # Format as YYMMDD
        except Exception as e:
            # Fallback to a safe default string in case of failure
            return f"Error: {str(e)}"

    def get_today_yyyymmdd(self):
        try:
            today = datetime.datetime.today()
            return today.strftime("%Y%m%d")
        except Exception as e:
            # Fallback in case of unexpected datetime issues
            raise RuntimeError(f"Failed to get today's date: {e}")

    def get_today_yyyymm(self):
        try:
            today = datetime.datetime.today()
            return today.strftime("%Y%m")
        except Exception as e:
            # Fallback in case of unexpected datetime issues
            raise RuntimeError(f"Failed to get today's date: {e}")

    # Function to generate a string of spaces with validation
    def generate_spaces(self, length: str):
        # Validate input type
        if not isinstance(length, str):
            raise TypeError("Length must be an str.")

        spaces_count = int(length) if length.isdigit() else len(length)

        # Kode lama: return " " * int(length)
        return " " * spaces_count

    def generate_spaces_int(self, length: int):
        # Validate input type
        if not isinstance(length, int):
            raise TypeError("Length must be an int.")

        # Validate boundary
        if length < 0:
            raise ValueError("Length cannot be negative.")

        # Generate spaces
        return " " * length

    def pad_with_spaces(self, text: str, length: int):
        # type data validation
        if not isinstance(text, str):
            raise TypeError("text parameter must be string.")
        if not isinstance(length, int):
            raise TypeError("length parameter must be integer.")
        if length < 0:
            raise ValueError("length parameter cannot negatif.")

        # check length
        if len(text) >= length:
            return text

        # count total space needed
        spaces_needed = length - len(text)
        return text + (" " * spaces_needed)

    def to_padded_number(self, length: int, number: int):
        try:
            # Validasi tipe
            if not isinstance(length, int) or not isinstance(number, int):
                raise TypeError("length and number must be integer type.")

            # Validasi nilai length
            if length <= 0:
                raise ValueError("length must be positive number.")

            # Ubah ke string dan lakukan padding
            num_str = str(abs(number))  # abs untuk aman jika number negatif
            if len(num_str) > length:
                raise ValueError("length number greater than length requested")

            return num_str.zfill(length)
        except (TypeError, ValueError):
            raise

    def to_fixed_numeric(self, value, length: int):
        """Normalisasi field NAM menjadi angka zero-padded dengan panjang tetap."""
        text_value = str(value).strip()
        if text_value[:1] in {"C", "D"}:
            text_value = text_value[1:]
        if not text_value:
            text_value = "0"
        if not text_value.isdigit():
            raise ValueError(f"NAM value must contain digits only: {value!r}")
        if len(text_value) > length:
            raise ValueError(
                f"NAM value length {len(text_value)} exceeds requested length {length}"
            )
        return text_value.zfill(length)

    def get_data_fetch_one(self):
        logging.info('START Get Data merchant pwc')
        try:
            way4 = Way4DB()
            merchant_query = (
                "SELECT DISTINCT connection_acq "
                "FROM sw_replicate.on_doc_transaction "
                "WHERE connection_acq IS NOT NULL"
            )
            logging.info('Get data Way4')
            logging.info("executing query: %s", merchant_query)

            records = way4.get_data_way4(sql=merchant_query)

            if records is None:
                logging.warning('Way4 get data return None')
                records = []

            logging.info('Final result: %s', records)
            return records

        except Exception:
            logging.exception('Gagal ambil data Way4')
            raise

    def combine_mapped_posting_result(
        self,
        mapped_rintis,
        mapped_way4,
        output_directory,
        filename=None,
    ):
        mapped_rintis = list(mapped_rintis or [])
        mapped_way4 = list(mapped_way4 or [])

        if filename is None:
            timestamp = self.get_timestamp_POST()
            filename = f"POSTFLIN_{timestamp}.txt"

        if not isinstance(filename, str) or not filename.endswith(".txt"):
            raise ValueError("filename must be string ext .txt")
        if ".." in filename or "/" in filename or "\\" in filename:
            raise ValueError("filename not be path")

        os.makedirs(output_directory, exist_ok=True)
        full_path = os.path.join(output_directory, filename)

        body = mapped_rintis + mapped_way4
        body_lines = [
            line
            for block in body
            for line in block.splitlines()
            if line
        ]
        header = self.generate_hr_record()
        trailer = self.generate_tr_record(
            sequence_number=len(body_lines) + 2,
            posting_lines=body_lines,
        )
        posting_file = header + "".join(body) + trailer
        with open(full_path, "w", encoding="utf-8") as posting_files:
            posting_files.write(posting_file)

        if not os.path.isfile(full_path):
            raise FileNotFoundError(f"Combine posting file not found: {full_path}")

        logging.info(
            "Combine posting created: %s | rintis: %s | way4: %s | size: %s",
            full_path,
            len(mapped_rintis),
            len(mapped_way4),
            os.path.getsize(full_path),
        )
        return full_path

    def map_record_to_posting_block(self, rec, mid):
        mid = str(mid).strip()
        if not mid:
            raise ValueError("Mid result mapping is not null")
        return (
            self.generate_hs_record(rec, str(mid).strip())
            + self.generate_dt_record(rec)
            + self.generate_oa_record(rec)
            + self.generate_ts_record(rec, str(mid).strip())
        )

    def map_way4_to_posting_records(self, records):

        columns = (
            "institutionbranch_acq",
            "sourceregnum",
            "pan",
            "period",
            "periodtime",
            "stan",
            "rrn",
            "institution_acq",
            "transactiontype",
            "settlementdate",
            "settlementcurrency",
            "reconamount",
            "transactioncurrency",
            "transactionchargeamount",
            "merchantid",
            "auth_code",
            "rc",
            "reversalseq",
        )

        def text(value, default=""):
            return default if value is None else str(value).strip()

        def numeric(value, length):
            if value is None:
                return "0".zfill(length)
            value_text = format(value, "f") if hasattr(value, "as_tuple") else str(value)
            value_text = value_text.strip()
            if "." in value_text:
                integer, fraction = value_text.split(".", 1)
                if fraction.strip("0"):
                    value_text = str(int(round(float(value_text))))
                else:
                    value_text = integer
            value_text = value_text.lstrip("+") or "0"
            if not value_text.isdigit():
                raise ValueError(f"Invalid Way4 numeric value: {value!r}")
            return value_text[-length:].zfill(length)

        def processing_code(transaction_type):
            """Turunkan processing code 6 digit sesuai tabel mapping Way4."""
            raw_value = text(transaction_type).upper()
            first_two = raw_value[:2]
            supported_codes = {
                "00", "01", "09", "11", "12", "17", "19", "20", "21",
                "28", "29", "40", "92", "93", "94", "95", "96", "97",
            }
            if first_two in supported_codes:
                return first_two + "0000"

            text_mapping = {
                "RETAIL": "00",
                "PURCHASE": "00",
                "CHK_RCPT": "00",
                "CH PAYMENT": "00",
                "IB_TRF": "40",
                "TRANSFER": "40",
                "REFUND": "20",
                "CREDIT VOUCHER": "20",
                "WITHDRAWAL": "01",
                "CASH": "11",
                "DEPOSIT": "21",
                "LOAN": "92",
                "REDEMPTION": "94",
                "CONVENIENCE FEE": "97",
                "BALANCE INQUIRY": "00",
            }
            mapped_code = text_mapping.get(raw_value)
            if mapped_code is None:
                logging.warning(
                    'transactiontype Way4 belum dikenal, fallback purchase: %r',
                    transaction_type,
                )
                mapped_code = "00"
            return mapped_code + "0000"

        posting_records = []
        for row in records or []:
            values = dict(row) if isinstance(row, dict) else dict(zip(columns, row))

            mid = text(values.get("pan"))
            if not mid:
                logging.warning('Skip Way4 record without pan: %s', row)
                continue

            period = text(values.get("period"))
            settlement_date = text(values.get("settlementdate"), period)
            transaction_date = (settlement_date or period)[:8].ljust(8)
            transaction_time = text(values.get("periodtime"), "000000")
            transaction_time = transaction_time[:6].zfill(6)
            settlement_currency = text(
                values.get("settlementcurrency"), "360"
            )[:3].ljust(3)
            transaction_currency = text(
                values.get("transactioncurrency"), settlement_currency
            )[:3].ljust(3)
            pan = text(values.get("pan"))
            rrn = text(values.get("rrn"))
            source_regnum = text(values.get("sourceregnum"))
            institution_acq = text(values.get("institution_acq"))
            merchant_id = text(values.get("merchantid"), mid)
            authorization_code = text(values.get("auth_code"))
            response_code = text(values.get("rc"), "00")
            if not authorization_code:
                authorization_code = response_code
            reversal_sequence = values.get("reversalseq")
            reversal_flag = (
                "F"
                if reversal_sequence is not None and int(reversal_sequence) > 0
                else "N"
            )

            base_record = ReconRecordData(
                recon_header="DH",
                terminal_id=source_regnum[:16].ljust(16),
                retrieval_reference_number=rrn[:12].ljust(12),
                merchant_pan=pan[:19].ljust(19),
                transaction_date=transaction_date,
                transaction_time=transaction_time,
                processing_code=processing_code(values.get("transactiontype")),
                transaction_amount=numeric(values.get("reconamount"), 12),
                convenience_fee=numeric(
                    values.get("transactionchargeamount"), 9
                ),
                transaction_amount_currency=transaction_currency,
                merchant_type=" " * 4,
                merchant_criteria=" " * 3,
                acquiring_bank_code=institution_acq[:11].ljust(11),
                issuer_bank_code=" " * 11,
                forwarding_institution_id=" " * 11,
                response_code=response_code[:2].ljust(2),
                customer_pan=pan[:28].ljust(28),
                invoice_data=rrn[:20].ljust(20),
                approval_code=authorization_code[:6].ljust(6),
                message_type_indicator="0200",
            )
            rec = SimpleNamespace(
                **base_record.dict(),
                outlet_number=merchant_id,
                batch_capture_date=period,
                batch_currency=settlement_currency,
                reversal_flag=reversal_flag,
                stan=text(values.get("stan")),
                transactiontype=text(values.get("transactiontype")),
            )
            posting_records.append((rec, mid))

        return posting_records

    def map_way4_query_result(self, records):
        return [
            self.map_record_to_posting_block(rec, mid)
            for rec, mid in self.map_way4_to_posting_records(records)
        ]
