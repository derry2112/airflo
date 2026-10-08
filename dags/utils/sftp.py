"""Local adapters; these are not the proprietary utilities shown in the reference."""

from airflow.providers.ftp.hooks.ftp import FTPHook
from airflow.providers.sftp.hooks.sftp import SFTPHook


class SFTPHookOcbc(SFTPHook):
    """SFTP provider exposed under the utility name used by acquiring DAGs."""


class SftpHook:
    """Select a transfer hook without changing existing FTP connection types."""

    def get_hook(self, connection_type, connection_id):
        if connection_type == "FTP":
            return FTPHook(ftp_conn_id=connection_id)
        if connection_type == "SFTP":
            return SFTPHookOcbc(ssh_conn_id=connection_id)
        raise ValueError(f"Connection type tidak didukung: {connection_type}")
