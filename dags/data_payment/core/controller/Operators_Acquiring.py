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
from data_payment.core.connection.ConnectionDB import CommonSQL
from data_payment.core.connection.BaseConnection import DBConnection
from data_payment.core.connection.Connection4WAY import Way4DB, PwcDB
from data_payment.core.connection.ConnectionACQ import ACQ_DB


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
                    generated_posting_files = split_class.split_rintis_qr_recon(str_file_name=source_file, str_result_name=mti_source_file, str_result_pwc=pwc_source_file)
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
                    # Kode lama: file PWC diberi nama .chk walaupun sumber upload
                    # berisi data hasil split, bukan check file.
                    # remote_file_pwc = os.path.join(self.target_path, chk_name_mti).replace("\\", "/")
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

    # Keterangan: method pendukung dibuat agar task get_way4_data pada BaseDags_QRRecon dapat dimuat.
    def get_way4_data(self):
        logging.info('========== START GET WAY4 DATA ==========')
        split_class = SplitClass()
        records = split_class.get_data_fetch_one()
        logging.info('========== END GET WAY4 DATA: %s ==========', records)
        return records


class SplitClass():
    def __init__(self, kwares_db_source):
        self.kwares_db_source = kwares_db_source
        logging.info(f"config: kwares_db_source={self.kwares_db_source}")
        
    def split_rintis_qr_recon(self, str_file_name, str_result_name, str_result_pwc):
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

                        # Keterangan: hasil query disimpan pada mid_records. Variabel
                        # mid baru dibuat setelah dipastikan query mengembalikan data.
                        # Kode lama: if not mid:
                        if not mid_records:
                            logging.info('MID not found for MPAN [%s]', mpan)
                            continue

                        mid = mid_records[0][0]
                        logging.info('MID record:%s', mid_records)
                        mid = str(mid).strip()

                        logging.info('[QR_RECON] MID selected | mpan:%s | type:%s | lenght:%s', mpan,mid,type(mid).__name__,len(str(mid)))

                        filename_datetime = self.get_timestamp_POST()
                        # Keterangan: gunakan underscore sebagai pemisah POSTFLIN
                        # dan timestamp agar nama lokal serta remote menjadi seragam.
                        # Kode lama: filename = f"POSTFLIN.{filename_datetime}.txt"
                        filename = f"POSTFLIN_{filename_datetime}.txt"

                        logging.info('[QR_RECON] Calling create_posting_file | filename:%s | mpan:%s | mid:%s | target_split:%s ', filename,mpan,mid,str_result_pwc)

                        generated_posting_file = self.create_posting_file(
                            filename,
                            rec,
                            mid,
                            target_split=str_result_pwc,
                        )
                        if generated_posting_file not in generated_posting_files:
                            generated_posting_files.append(generated_posting_file)
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
                    logging.info('==========SPLIT PROCESSED END QR RECO ==========')
            return generated_posting_files
        except Exception:
            logging.info('Error split rintis RAW')
            raise

    def get_data_merchant_pwc_qr(self):
        db = DBConnection(**self.kwares_db_source)

        # get data new merchant
        get_merchant_query = "SELECT mpan FROM merchant_tm"
        logging.info('Get Data Merchant PWC')
        logging.info(f'Executing query: {get_merchant_query}')

        merchants = db.execute_fetch_many(get_merchant_query)

        logging.info('Finish Get Data Merchant PWC')
        return merchants

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
                record = []

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

            # Keterangan: jalankan query dengan bind parameter :mpan. Sebelumnya
            # object PwcDB hanya dibuat sehingga variabel records belum memiliki nilai.
            records = pwc.get_fetch_one(sql=sql, params={"mpan": mpan})

            if records is None:
                logging.warning('pwc get data return None')
                records = []
            logging.info('records result acceptor point: %s', records)
            return records
        except Exception as e:
            logging.exception('Gagal ambil data MID dari PWC')
            raise

    def create_posting_file(self, filename, rec: ReconRecordData, mid: str, target_split: str):
        try:
            logging.info("================ CREATE POSTING FILE ====================")

            logging.info('filename:%s', filename)
            logging.info('filename type: %s', type(filename))
            logging.info('mid:%s', mid)
            logging.info('rec.to_line', rec.to_line())
            if not isinstance(filename, str) or not filename.endswith(".txt"):
                raise ValueError(f"Filename must be string type and .txt extention. Got: {filename!r}, type:{type(filename)}")

            if ".." in filename or "/" in filename or "\\" in filename:
                raise ValueError("filename cannot contain a path")

            # full_path = os.path.join(target_split)
            full_path = os.path.join(os.path.dirname(target_split),filename)

            header_record = self.generate_hr_record()
            trailer_record = self.generate_tr_record()

            logging.info('full path:%s', full_path)
            logging.info('header_record:%s',header_record)
            logging.info('trailer_record:%s',trailer_record)

            # Keterangan: normalisasi MID satu kali agar nilai yang sama digunakan
            # secara konsisten oleh generator HS dan TS.
            mid = str(mid).strip()

            # Kode lama: merchant_header_record = self.generate_hs_record(rec, str(mid).strip())
            merchant_header_record = self.generate_hs_record(rec, mid) # HS
            transaction_record = self.generate_dt_record(rec) # DT
            # Kode lama: other_amounts_record = self.generate_oa_record()
            other_amounts_record = self.generate_oa_record(rec) # OA
            # Kode lama: merchant_batch_record = self.generate_ts_record(rec, str(mid).strip())
            merchant_batch_record = self.generate_ts_record(rec, mid) # TS

            # merchant_header_record = self.generate_hs_record(rec, mid) # HS
            # transaction_record = self.generate_dt_record(rec) # DT
            # other_amounts_record = self.generate_oa_record() # OA
            # merchant_batch_record = self.generate_ts_record(rec, mid) # TS

            logging.info('merchant_header_record:%s', merchant_header_record)
            logging.info('transaction_record:%s', transaction_record)
            logging.info('other_amounts_record:%s', other_amounts_record)
            logging.info('merchant_batch_record:%s', merchant_batch_record)

            full_containt_record = merchant_header_record + transaction_record + other_amounts_record + merchant_batch_record

            if os.path.isfile(full_path):
                # Keterangan: beberapa transaksi dapat memperoleh timestamp nama
                # file yang sama. Pertahankan HR lama, lepaskan TR lama, tambahkan
                # blok HS/DT/OA/TS baru, kemudian tulis kembali satu TR di akhir.
                with open(full_path, "r", encoding="utf-8") as existing_file:
                    existing_containt = existing_file.read()
                if existing_containt.endswith(trailer_record):
                    existing_containt = existing_containt[:-len(trailer_record)]
                full_containt = existing_containt + full_containt_record + trailer_record
            else:
                # Kode lama untuk transaksi pertama tetap digunakan: HR di awal,
                # satu blok transaksi, lalu TR di akhir file.
                # full_containt = header_record + full_containt_record + trailer_record
                full_containt = header_record + full_containt_record + trailer_record

            # logging.info('full_containt_record:%s',full_containt_record)
            logging.info('full_containt:%s', full_containt)
            logging.info("Create file : %s", full_path)

            with open(full_path, "w", encoding="utf-8") as f:
                # Keterangan: gunakan variabel full_containt yang dibentuk dari
                # gabungan record HR, HS, DT, OA, TS, dan TR pada baris sebelumnya.
                # Kode lama: f.write(full_contain)
                f.write(full_containt)

            file_exists = os.path.isfile(full_path)
            logging.info("File generated: %s", full_path)
            logging.info("File Exists: %s", file_exists)

            if not file_exists:
                raise FileNotFoundError(f"Generated posting file not found:{full_path}")

            # Keterangan: path ini diteruskan sampai ke store_file() agar file yang
            # diunggah adalah hasil generator, bukan pwc_source_file mentah.
            return full_path

        except Exception as e:
            logging.exception(e)
            raise
            # print(f"Error when create posting file: {e}")
            # return False

    def generate_hr_record(self):
        record_type = "HR"

        last_sequence_number = 0
        this_sequence = last_sequence_number + 1

        sequence_name = self.pad_with_spaces(self.to_padded_number(8,this_sequence), 8)

        bank_code_id = "ID7339"
        institution_ref = self.generate_spaces(bank_code_id)

        today_yymmdd = self.pad_with_spaces(self.get_today_yymmdd(), 6)
        ptidn = self.pad_with_spaces(self.to_padded_number(6,this_sequence), 6)
        file_ref_value = f"PTPN{ptidn} {today_yymmdd}"

        file_ref = self.pad_with_spaces(file_ref_value, 16)
        processDate = self.pad_with_spaces(self.get_timestamp_POST(), 14)
        tokenization_indicator = "C"

        # Merging String
        result = record_type + sequence_name + institution_ref + file_ref + processDate + tokenization_indicator

        return result + "\n"

    def generate_tr_record(self):
        record_type = "TR"

        last_sequence_number = 0
        this_sequence = last_sequence_number + 1

        sequence_in_file = self.pad_with_spaces(str(this_sequence), 8)

        institution_identification = self.generate_spaces("6")
        file_sender = self.generate_spaces_int(6)
        count_debit = self.pad_with_spaces("1000000", 8)
        count_credit = self.pad_with_spaces("500000", 8)
        amount_debit = self.pad_with_spaces("2000000", 18)
        amount_credit = self.pad_with_spaces("1500000", 18)

        # Merging String
        result = record_type + sequence_in_file + institution_identification + file_sender + count_debit + count_credit + amount_debit + amount_credit

        return result

    def generate_hs_record(self, rec: ReconRecordData, mid: str):
        logging.info('========== GENERATE HS RECORD ==============')
        logging.info('MID:%s', mid)
        logging.info('REC:%s', rec)
        logging.info('Terminal ID :%s', rec.terminal_id)

        record_type = "HS"

        last_sequence = 0
        new_sequence = last_sequence + 1

        last_batch = 0
        new_batch = last_batch + 1

        record_sequence = self.to_padded_number(8, new_sequence)
        merchant_number = self.pad_with_spaces(mid, 15)
        outlet_number = self.pad_with_spaces(mid, 15)
        terminal_id = self.pad_with_spaces(rec.terminal_id, 15)
        batch_number = self.to_padded_number(8, new_batch)
        batch_capture_date = self.pad_with_spaces(self.get_today_yyyymmdd(), 8)
        batch_datetime = self.pad_with_spaces(self.get_timestamp_POST(), 14)
        batch_currency = self.pad_with_spaces("360", 3)
        batch_type = "P"

        logging.info('HS record_type = [%s] | lenght_2 = %s', record_type, len(record_type))
        logging.info('HS record_sequence = [%s] | lenght_8 = %s', record_sequence, len(record_sequence))
        logging.info('HS merchant_number = [%s] | lenght_15 = %s', merchant_number, len(merchant_number))
        logging.info('HS outlet_number: [%s] | lenght_15 = %s', outlet_number, len(outlet_number))
        logging.info('HS terminal_id: [%s] | lenght_15 = %s', terminal_id, len(terminal_id))
        logging.info('HS batch_number = [%s] | lenght_8', batch_number, len(batch_number))
        logging.info('HS batch_capture_date = [%s] | lenght_8', batch_capture_date, len(batch_capture_date))
        logging.info('HS batch_datetime = [%s] | lenght_14', batch_datetime, len(batch_datetime))
        logging.info('HS batch_currency = [%s] | lenght_3', batch_currency, len(batch_currency))
        logging.info('HS batch_type = [%s] | lenght_1', batch_type, len(batch_type))

        # Merging String
        result = record_type + record_sequence + merchant_number + outlet_number + terminal_id + batch_number + batch_capture_date + batch_datetime + batch_currency + batch_type
        logging.info('HS final result:[%s]', result)
        logging.info('HS LENGHT = %s', len(result))

        if len(result)!=89:
            raise ValueError(f"invalid HS RECORD lenght:{len(result)},expected 89")

        return result + "\n"

    def generate_dt_record(self, rec: ReconRecordData):
        record_type = "DT"

        last_sequence = 0
        new_sequence = last_sequence + 1

        last_trx_batch = 0
        new_trx_batch = last_trx_batch + 1

        # dummy (temporary)
        voucher_generated = f"VOUCHER{new_sequence}"

        record_sequence_in_file = self.to_padded_number(8, new_sequence)
        transaction_sequence_in_batch = self.to_padded_number(7, new_trx_batch)
        service_type = "0"
        voucher_number = self.pad_with_spaces(voucher_generated, 8)
        # Keterangan: gunakan customer PAN dari record Rintis sebagai card number.
        # Kode lama: card_number = self.generate_spaces_int(22)
        card_number = self.pad_with_spaces(rec.customer_pan, 22)
        expiry_date = self.generate_spaces_int(6)
        processing_date = self.pad_with_spaces(self.get_today_yyyymm(), 6)
        reversal_flag = "N"
        authorization_flag = "A"
        post_date = self.pad_with_spaces("10000113A110", 12)
        post_entry_mode = self.pad_with_spaces("A2", 4)
        post_condition_code = self.pad_with_spaces("00", 2)
        transaction_datetime = self.pad_with_spaces(f"{rec.transaction_date}{rec.transaction_time}", 14)
        transaction_amount = self.pad_with_spaces(rec.transaction_amount, 18)
        transaction_sign = "C"
        transaction_currency = self.pad_with_spaces(rec.transaction_amount_currency, 3)
        currency_exponent = self.generate_spaces_int(1)
        reversal_reason_code = self.generate_spaces_int(2)
        replacement_amounts = self.generate_spaces_int(18)
        # Keterangan: approval_code tersedia langsung pada record Rintis.
        # Kode lama: authorization_code = self.pad_with_spaces("RC", 6)
        authorization_code = self.pad_with_spaces(rec.approval_code, 6)
        service_code = self.generate_spaces_int(3)
        single_message_indicator = "Y"

        # Merging String
        result = record_type + record_sequence_in_file + transaction_sequence_in_batch + service_type + voucher_number + card_number + expiry_date + processing_date + reversal_flag + authorization_flag + post_date + post_entry_mode + post_condition_code + transaction_datetime + transaction_amount + transaction_sign + transaction_currency + currency_exponent + reversal_reason_code + replacement_amounts + authorization_code + service_code + single_message_indicator
        return result + "\n"

    # Kode lama: def generate_oa_record(self):
    def generate_oa_record(self, rec: ReconRecordData):
        record_type = "OA"

        last_sequence = 0
        new_sequence = last_sequence + 1

        sequence = self.to_padded_number(8, new_sequence)

        record_sequence = sequence
        voucher_number = sequence
        tip_amount = self.generate_spaces_int(18)
        cashback_amount = self.generate_spaces_int(18)
        # Keterangan: convenience fee Rintis dipetakan ke field fee dan dipenuhi
        # dengan spasi hingga panjang record OA yang dibutuhkan.
        # Kode lama: fee = self.generate_spaces_int(18)
        fee = self.pad_with_spaces(rec.convenience_fee, 18)
        surcharge_fee = self.generate_spaces_int(18)
        # Keterangan: transaction amount Rintis digunakan sebagai billing amount.
        # Kode lama: billing_amount = self.generate_spaces_int(18)
        billing_amount = self.pad_with_spaces(rec.transaction_amount, 18)
        billing_currency = self.pad_with_spaces("360", 3)
        conversion_rate = self.generate_spaces_int(12)
        rate_exponent = self.generate_spaces_int(2)
        reversed_for_future_use = ""
        # Keterangan: invoice_data dipakai sebagai external reference karena berasal
        # dari transaksi yang sama dan muat pada field 24 karakter.
        # Kode lama: external_ref_id = self.generate_spaces_int(24)
        external_ref_id = self.pad_with_spaces(rec.invoice_data, 24)
        dcc_indicator = "Y"
        reversed_for_future_use2 = ""

        # Merging String
        result = record_type + record_sequence + voucher_number + tip_amount + cashback_amount + fee + surcharge_fee + billing_amount + billing_currency + conversion_rate + rate_exponent + reversed_for_future_use + external_ref_id + dcc_indicator + reversed_for_future_use2

        return result + "\n"

    def generate_ts_record(self, rec: ReconRecordData, mid: str):
        record_type = "TS"

        last_sequence = 0
        new_sequence = last_sequence + 1

        last_batch = 0
        new_batch = last_batch + 1

        sequence_in_file = self.to_padded_number(8, new_sequence)
        merchant_number = self.pad_with_spaces(mid, 15)
        outlet_number = self.pad_with_spaces(mid, 15)
        terminal_id = self.pad_with_spaces(rec.terminal_id, 15)
        batch_number = self.pad_with_spaces(str(new_batch), 8)
        batch_datetime = self.pad_with_spaces(self.get_timestamp_POST(), 14)
        record_count_debit = self.generate_spaces_int(6)
        net_amount_debit = self.generate_spaces_int(18)
        record_count_credit = self.generate_spaces_int(6)
        net_amount_credit = self.generate_spaces_int(18)

        # Merging String
        result = record_type + sequence_in_file + merchant_number + outlet_number + terminal_id + batch_number + batch_datetime + record_count_debit + net_amount_debit + record_count_credit + net_amount_credit
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

        # Keterangan: pemanggil lama mengirim dua bentuk nilai:
        # - string angka, misalnya "6", berarti menghasilkan 6 spasi;
        # - string teks, misalnya "ID7339", berarti menghasilkan spasi sebanyak
        #   panjang teks tersebut. Dengan demikian kedua pemanggilan tetap didukung.
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

    def get_data_fetch_one(self):
        logging.info('START Get Data merchant pwc')
        logging.info(f"get_data: kwares_db_source={self.kwares_db_source}")
        try:
            way4 = Way4DB()
            merchant_query = (
                "SELECT DISTINCT connection_acq"
                "FROM sw_replicate.on_doc_transaction"
                "WHERE connection_acq IS NOT NULL"
            )
            logging.info('Get data Way4')
            logging.info("executing query: %s", merchant_query)

            records = way4.get_data_way4(sql=merchant_query)

            if records is None:
                logging.warning('Way4 get data return None')
                records = []
            logging.info('final result: %s', records)
            return records
        except Exception as e:
            logging.exception(e)
            raise
