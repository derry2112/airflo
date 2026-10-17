import ftplib
import json
import logging
from airflow.providers.ftp.hooks.ftp import FTPHook
from airflow.sdk import BaseHook

from dags.utils.conjur_secret_manager import ConjurSecretManager
from dags.utils.models.connection_model import ConnectionModel
from typing import cast

logger = logging.getLogger(__name__)

class FTPHookOcbc(FTPHook):
    default_conn_name = "ftp_default"
    def __init__(self, ftp_conn_id: str = default_conn_name) -> None:
        super().__init__(ftp_conn_id=ftp_conn_id)
        self.conjur_manager = ConjurSecretManager()

    def get_conn(self, ftp_conn_id: str = None) -> ftplib.FTP:
        """Return an FTP connection object."""
        conn_id = ftp_conn_id or self.ftp_conn_id

        conn_airflow = BaseHook.get_connection(conn_id)
        extra_json = json.loads(conn_airflow.extra or "{}")
        connection_obj = ConnectionModel(
            username_dap=extra_json.get("username_dap", ""),
            password_dap=extra_json.get("password_dap", ""),
            host=conn_airflow.host,
            port=conn_airflow.port,
            schema=conn_airflow.schema
        )

        # CONJUR VARIABLE PROD VERSION
        username = self.conjur_manager.get_conjur_value(connection_obj.username_dap) if extra_json.get("username_dap") else conn_airflow.login
        password = self.conjur_manager.get_conjur_value(connection_obj.password_dap) if extra_json.get("password_dap") else conn_airflow.password

        if self.conn is None:
            params = self.get_connection(conn_id)
            pasv = params.extra_dejson.get("passive", True)
            encoding = params.extra_dejson.get("encoding")
            self.encoding = encoding
            if encoding:
                self.conn = ftplib.FTP(encoding=encoding)  # nosec: B321
            else:
                self.conn = ftplib.FTP()  # nosec: B321
            if params.host:
                port = params.port if params.port is not None else ftplib.FTP_PORT
                if params.port is not None:
                    port = params.port
                logger.info("Connecting via FTP to %s:%d", params.host, port)
                self.conn.connect(params.host, port)
                params.password = cast("str", password)
                self.conn.login(username, password)
            self.conn.set_pasv(pasv)

        return self.conn
