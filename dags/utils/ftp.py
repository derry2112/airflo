import logging
import os
from pathlib import Path

from dags.utils.hooks.FTPHookOcbc import FTPHookOcbc
from dags.utils.str_utils import StrUtils
from utils.telemetry.tracing import tracer
import polars as pl

class FtpHook:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.str_utils = StrUtils()

    def put(self, file_path:str="", destination_path:str="", ftp_conn_id:str="", delete_local:bool=False):
        """
        Upload file to FTP server

        :param file_path: Path of local file in OCP
        :type file_path: str
        :param destination_path: Destination Path in FTP.
        :type destination_path: str
        :param ftp_conn_id: FTP Connection ID
        :type ftp_conn_id: str
        :param delete_local: If True, delete existing file after uploading
        :type delete_local: bool
        """
        ftp_hook = FTPHookOcbc(ftp_conn_id=ftp_conn_id)

        with ftp_hook.get_conn() as ftp_conn:
            with open(file_path, "rb") as file:
                ftp_conn.storlines(
                    f"STOR {destination_path}",
                    file
                )
            self.logger.info(f"Uploaded file to: {destination_path}")

            # If delete_local mode, delete existing file first
            if delete_local:
                try:
                    os.remove(file_path)
                    self.logger.info(f"File {file_path} is removed from local OCP")
                except Exception as e:
                    # File might not exist, which is fine
                    self.logger.info(f"File {destination_path} not found or delete failed: {e}")


    def read_file(self, remote_path:str="", local_path:str="", ftp_conn_id:str="", separator:str="|") -> pl.DataFrame:
        """
        Docstring for download file to airflow Persistent Volume and read its value (and remove the file from local)

        :param remote_path: File path in remote
        :type remote_path: str
        :param local_path: Destination Path in persistent volume.
        :type local_path: str
        :param ftp_conn_id: FTP Connection ID
        :type ftp_conn_id: str
        """
        try:
            if self.str_utils.isEmpty(remote_path) or self.str_utils.isEmpty(local_path):
                raise ValueError("Remote Path and Local Past must exist")

            self.__download_file__(remote_path=remote_path, local_path=local_path, ftp_conn_id=ftp_conn_id)
            df = pl.read_csv(local_path, separator=separator)

            self.__delete_local__(local_path=local_path)

            return df
        except Exception as ex:
            raise ValueError(ex)




    def __download_file__(self, remote_path:str="", local_path:str="", ftp_conn_id:str=""):
        """
        Docstring for download file to airflow Persistent Volume

        :param remote_path: File path in remote
        :type remote_path: str
        :param local_path: Destination Path in persistent volume.
        :type local_path: str
        :param ftp_conn_id: FTP Connection ID
        :type ftp_conn_id: str
        """
        folder_dir = os.path.dirname(local_path)
        self.logger.info(f"File will be stored in {folder_dir}")

        if not os.path.exists(folder_dir):
            self.logger.info(f"Folder not exist. Creating folder: {folder_dir}")
            os.makedirs(folder_dir, exist_ok=True)

        ftp_hook = FTPHookOcbc(ftp_conn_id=ftp_conn_id)
        ftp_hook.retrieve_file(remote_path, local_path)



    def __delete_local__(self, local_path:str=""):
        """
        Docstring for deleting file in local OCP.

        :param local_path: File path in local OCP
        :type local_path: str
        """
        file = Path(local_path)
        if file.exists():
            os.remove(local_path)
            self.logger.info(f"{local_path} is deleted")
        else:
            self.logger.info(f"{local_path} is not exist")
