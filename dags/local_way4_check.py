"""Explicit local simulation; does not change production source or send files."""
from datetime import datetime

from airflow.decorators import dag, task


def run_local_check():
    import hashlib
    import json
    import tempfile
    from pathlib import Path
    from unittest.mock import patch

    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from data_payment.core.controller import Operators_Acquiring as operators

    hook = PostgresHook(postgres_conn_id="db_rekon_uat")
    connection = hook.get_connection("db_rekon_uat")
    if connection.host != "postgres" or connection.schema != "db_rekon_uat":
        raise RuntimeError("This check requires the local Compose postgres/db_rekon_uat.")

    # Synthetic enrichment exposed as CTEs: no MSSQL server or production rows.
    fixture_cte = """
        WITH qris_transaction_log_tt AS (
            SELECT DISTINCT rrn, merchantid AS mid,
                '10'::text AS from_account_type, '20'::text AS to_account_type,
                '9360002810000000000'::text AS mpan,
                '5411'::text AS merchant_type, rrn AS invoice_number,
                0::numeric AS fee
            FROM sw_replicate.on_doc_transaction
            WHERE add_data = 'LOCAL_DUMMY_WAY4_QR_V1'
        ), merchant_tm AS (
            SELECT 'DUMMYMERCHANT001'::text AS mid, 'UMI'::text AS merchant_criteria
        )
    """

    class LocalDBConnection:
        def __init__(self, **kwargs):
            self.config = kwargs

        def execute_fetch_many(self, sql, params):
            return hook.get_records(
                fixture_cte + sql.replace(":rrns", "%(rrns)s"),
                parameters=params,
            )

    cfg = {"type": "postgres", "connection_id": "db_rekon_uat"}
    output_dir = Path(tempfile.mkdtemp(prefix="way4-local-", dir="/opt/airflow"))
    replication = operators.Replication(kwargs_db_source=cfg)
    splitter = operators.SplitClass(cfg)
    # Patch only within this local test process. Existing modules on disk stay intact.
    with patch.object(operators, "DBConnection", LocalDBConnection):
        rows = replication.get_way4_data_new(save_result_file=False)
        rows = [row for row in rows if row[0] == "DUMMYMERCHANT001"]
        if len(rows) != 4:
            raise AssertionError(f"Expected 4 dummy transactions for yesterday, got {len(rows)}. Run seed 04 first.")
        mapped = splitter.map_way4_to_posting_records_new(rows)
        if len(mapped) != 4:
            raise AssertionError("Not all dummy transactions were mapped")
        output = splitter.create_posting_batch_file(
            "POSTFLIN_LOCAL_DUMMY.txt", mapped,
            target_split=str(output_dir / "input.txt"),
        )
    data = Path(output).read_bytes()
    if not data:
        raise AssertionError("Generated posting file is empty")
    result = {
        "simulation_only": True, "transactions": len(mapped),
        "output": output, "bytes": len(data),
        "sha256": hashlib.sha256(data).hexdigest(),
        "uploaded": False, "posted": False,
    }
    print(json.dumps(result, indent=2))
    return result


@dag(
    dag_id="local_way4_mapping_check", schedule=None,
    start_date=datetime(2025, 1, 1), catchup=False,
    tags=["local", "simulation"], default_args={"retries": 0},
)
def local_way4_mapping_check():
    @task
    def check_mapping():
        return run_local_check()

    check_mapping()


local_way4_mapping_check()
