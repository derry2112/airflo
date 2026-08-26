import os
import sys

sys.path.insert(1, "/data/airflow/nfs/dags")

from data_payment.core.basedags.BaseDags_QRRecon import replication


FILE_PATH = os.path.dirname(os.path.realpath(__file__))
WORKFLOW_NAME = FILE_PATH.split("/")[-1]
TAGS = FILE_PATH.split("/")[-3:-1]

tags = ["data_payment", "acquiring", "splitfile"]
tags.extend(TAGS)

partner_rintis = {
    "workflow_name": WORKFLOW_NAME,
    "kwares_db_source": {
        "type": "postgres",
        "workflow_name": WORKFLOW_NAME,
        "connection_id": "db_acq_psql",
        "chunksize": 10000,
    },
    "source_connection": "FTP_EBSP",
    "source_path": "/ACQ/MTI/ClearingRintisQRIS/",
    "source_connection_type": "FTP",
    "destination_connection": "FTP_MTI",
    "destination_path": "/sftpdata/mti/mti/fromocbc/FileClearing/Rintis/",
    "destination_connection_type": "FTP",
    "format_date": "%Y%m%d",
    "resul_local_path": "/data/airflow/nfs/artifacts/data_payment/acquiring/result/rintis/",
    "resul_local_path_PWC": "/data/airflow/nfs/artifacts/data_payment/acquiring/result/rintisPWC",
    "resul_local_path_way4": "/data/airflow/nfs/artifacts/data_payment/acquiring/result/way4/",
    "fetch_date": -1,  # tanggal pengambilan datanya di kapan (-1 = H-1, 0 = H-0)
    # deny
    "destination_connection_PWC_POST": "SFTP_PWC_POST",
}

config = {
    "workflow_name": "etl_OCBC_MTI_RINTIS_ProcessFileRintisQRRecon",
    "owner": "raymundus.liputre",
    "kwares_db_source": partner_rintis.get("kwares_db_source"),
    "schedule_interval": "0 5-23/2 * * *",
    "start_date": "2023-01-01",
    "tags": tags,
    "source_connection": partner_rintis.get("source_connection"),
    "source_path": partner_rintis.get("source_path"),
    "source_connection_type": partner_rintis.get("source_connection_type"),
    "destination_connection": partner_rintis.get("destination_connection"),
    "destination_path": partner_rintis.get("destination_path"),
    "destination_connection_type": partner_rintis.get("destination_connection_type"),
    "format_date": partner_rintis.get("format_date"),
    "result_local_path": partner_rintis.get("resul_local_path"),  # processed path yang dikirim ke FTP MII
    "result_local_path_PWC": partner_rintis.get("resul_local_path_PWC"),  # processed path yang dikirim ke FTP PWC
    "fetch_date": 0,  # partner_rintis.get("fetch_date")
    "local_path": "/data/airflow/nfs/artifacts/data_payment/acquiring/file/",  # unprocessed path
    "file_name_mask": "QR_RECON_*.dsj_ISS",  # NOTES: .dsj akan di-replace dengan format date dari config ai/rintis di dalam operators
    "file_name_mask_outgoing": "",
    "custom_function": "split_rintis_qr_recon",  # hanya diisi apabila is_split = TRUE
    "is_split": True,
    # deny
    "destination_connection_PWC_POST": partner_rintis.get("destination_connection_PWC_POST"),
    "result_local_path_way4": partner_rintis.get("resul_local_path_way4"),
    "target_path": "/pwrcard/home/usr/data/",
    "target_path_onus": "/pwrcard/home/usr/data/onus/",
}

dags = replication(config)
globals()["etl_OCBC_MTI_RINTIS_ProcessFileRintisQRRecon"] = dags
