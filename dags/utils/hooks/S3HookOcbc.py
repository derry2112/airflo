"""S3 provider adapter used by the transcribed S3ArtefactHook."""

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import BaseHook


class S3HookOcbc(S3Hook):
    @property
    def conn_extra(self):
        return BaseHook.get_connection(self.aws_conn_id).extra
