"""Client DAG configuration fields and their defaults."""

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


class DbDestination:
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


class ConfigClient:
    workflow_name: str = None
    start_date: str = None
    tags: List[str] = None
    schedule_interval: str = None
    catchup: bool = False
    max_active_runs: int = 1
    base_artifacts: str = None
    local_artifacts: str = None
    data_pool: str = None
    destination_schema: str = None
    destination_table: str = None
    header_table: str = None
    truncate_destination: str = None
    custom_delete_query: str = None
    sp: str = None
    sp_name: str = None
    select_column: str = None
    column_destination: str = None
    mapping_type_data: str = None
    kwargs_db_destination: DbDestination = None
    default_args: DefaultArgs = None
    control_table: ControlTable = None
    config_ftp: ConfigFTP = None
    custom_query: str = None
    source_schema: str = None
    source_table: str = None
    column_width: dict[str, int] = None
