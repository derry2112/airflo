"""Master DAG configuration fields and their defaults."""

from datetime import timedelta
from typing import List


class DefaultArgs:
    owner: str = None
    retries: int = 2
    retry_delay: timedelta = timedelta(minutes=5)
    priority_weight: int = 10
    sla: timedelta = timedelta(hours=5)
    execution_timeout: timedelta = timedelta(minutes=300)
    timezone: str = "Asia/Jakarta"


class DbSource:
    connection_id: str = None
    chunksize: int = 500
    type_source: str = None


class ControlTable:
    CONNECTION_ID: str = None
    DATABASE_NAME: str = None
    DESTINATION_TABLE: str = None
    DESTINATION_DB: str = None
    CONTROL_TABLE: str = None


class ConfigFTP:
    CONNECTION_ID: str = None
    PATH_FILE_LOCAL: str = None
    PATH_FILE_REMOTE: str = None


class ConfigMaster:
    workflow_name: str = None
    dependencies_name: str = None
    parent_dag_id: str = None
    start_date: str = None
    tags: List[str] = None
    schedule_interval: str = None
    catchup: bool = False
    max_active_runs: int = 1
    base_artifacts: str = None
    source_schema: str = None
    source_table: str = None
    custom_query: str = None
    column_source: str = None
    key_column: str = None
    data_pool: int = 1
    kwargs_db_source: DbSource = None
    default_args: DefaultArgs = None
    child: str = None
    control_table: ControlTable = None
