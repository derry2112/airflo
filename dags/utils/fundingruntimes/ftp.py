from ftplib import error_perm

from dags.utils.hooks.FTPHookOcbc import FTPHookOcbc
from dags.utils.ftp import FtpHook

class FtpHookArjuna(FtpHook):
    def read_file(
        self, remote_path: str = "", local_path: str = "", ftp_conn_id: str = ""
    ) -> list:
        """
        Docstring for download file to airflow Persistent Volume and read its lines
        (and remove the file from local)

        :param remote_path: File path in remote
        :type remote_path: str
        :param local_path: Destination Path in persistent volume.
        :type local_path: str
        :param ftp_conn_id: FTP Connection ID
        :type ftp_conn_id: str
        """
        try:
            if self.str_utils.isEmpty(remote_path) or self.str_utils.isEmpty(local_path):
                raise ValueError("Remote Path and Local Path must exist")

            self.__download_file__(
                remote_path=remote_path, local_path=local_path, ftp_conn_id=ftp_conn_id
            )

            lines = []
            with open(local_path, "r", encoding="utf-8") as file:
                for line in file:
                    lines.append(line.strip())

            self.__delete_local__(local_path=local_path)

            return lines

        except Exception as ex:
            raise ValueError(f"Error processing file: {ex}")


    def check_file_if_exist(self, remote_path: str = "", ftp_conn_id: str = "") -> bool:
        """
        Checking file before run.

        Args:
            remote_path (str, optional): Full path of the remote file to check. Defaults to "".
            ftp_conn_id (str, optional): Airflow connection ID. Defaults to "ftp_default".

        Returns:
            bool: True if file exists, False otherwise.
        """
        try:
            ftp_hook = FTPHookOcbc(ftp_conn_id=ftp_conn_id)
            with ftp_hook.get_conn() as conn:
                conn.size(remote_path)
            return True
        except error_perm as e:
            if str(e).startswith("550"):
                return False
        except Exception:
            return False
