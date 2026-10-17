import json

from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.sdk import BaseHook

from dags.utils.conjur_secret_manager import ConjurSecretManager
from dags.utils.models.connection_model import ConnectionModel

class SFTPHookOcbc(SFTPHook):
    def __init__(self, ssh_conn_id = "sftp_default", host_proxy_cmd = None, use_managed_conn = True, *args, **kwargs):
        super().__init__(ssh_conn_id, host_proxy_cmd, use_managed_conn, *args, **kwargs)
        self.conjur_manager = ConjurSecretManager()


        conn_airflow = BaseHook.get_connection(ssh_conn_id)
        extra_json = json.loads(conn_airflow.extra or "{}")
        connection_obj = ConnectionModel(
            username_dap=extra_json.get("username_dap", ""),
            host=conn_airflow.host,
            port=conn_airflow.port,
            schema=conn_airflow.schema
        )

        # CONJUR VARIABLE PROD VERSION
        self.username = (
            self.conjur_manager.get_conjur_value_unparsed(connection_obj.username_dap)
            if extra_json.get("username_dap") else conn_airflow.login
        )

        if extra_json.get("password_dap"):
            connection_obj.password_dap = extra_json.get("password_dap")
            self.password = self.conjur_manager.get_conjur_value_unparsed(connection_obj.password_dap)
            print("Using Password DAP")
        elif extra_json.get("private_key_dap"):
            connection_obj.password_dap = extra_json.get("private_key_dap")
            private_key = self.conjur_manager.get_conjur_value_unparsed(connection_obj.password_dap)
            private_key = private_key.replace("\\n", "\n").replace("\\r", "\r").strip() #Replace \n with actual enter when arriving to self.pkey

            passphrase: str = None
            if extra_json.get("passphrase_dap"):
                passphrase_dap = extra_json["passphrase_dap"]
                passphrase = self.conjur_manager.get_conjur_value_unparsed(passphrase_dap)
                print("Passphrase DAP detected")

            self.pkey = self._pkey_from_private_key(private_key=private_key, passphrase=passphrase)
            print("Using Private Key DAP")

        # Jangan pakai di production ya
        elif extra_json.get("private_key_without_conjur"):
            self.username = extra_json["username_dap_without_conjur"]
            private_key = extra_json["private_key_without_conjur"]
            private_key = private_key.replace("\\n", "\n").replace("\\r", "\r").strip()

            passphrase_: str = ""
            if extra_json.get("passphrase_dap_without_conjur"):
                passphrase_ = extra_json["passphrase_dap_without_conjur"]
                print("Passphrase DAP detected")

            self.pkey = self._pkey_from_private_key(private_key=private_key, passphrase=passphrase_)
            print("Using Private Key DAP")
