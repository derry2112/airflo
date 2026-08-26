from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta, timezone
import sys

sys.path.insert(1, "/data/airflow/nfs/dags")
from data_payment.core.controller.Operators_Acquiring import Replication


def replication(config):
    workflow_name = config.get("workflow_name")
    owner = config.get("owner")
    schedule_interval = config.get("schedule_interval")
    tags = config.get("tags")
    tz = timezone(timedelta(hours=7))
    DR = Replication(**config)

    default_args = {
        "owner": owner,
        "retries": 0,
        "retry_delay": timedelta(minutes=1),
        "priority_weight": 10,
        "sla": timedelta(hours=1),
        "execution_timeout": timedelta(minutes=10),
        "timezone": "Asia/Jakarta",
    }

    dag = DAG(
        dag_id=workflow_name,
        default_args=default_args,
        start_date=datetime.strptime(config.get("start_date"), "%Y-%m-%d").astimezone(tz),
        schedule=schedule_interval,
        dagrun_timeout=timedelta(minutes=10),
        catchup=False,
        tags=tags,
        max_active_runs=1,
    )

    with dag:
        with TaskGroup(group_id=f"{workflow_name.upper()}") as step_general:
            start = EmptyOperator(
                task_id="START", trigger_rule=TriggerRule.ALL_DONE
            )

            check_chk = ShortCircuitOperator(
                task_id="check_chk",
                python_callable=DR.check_chk_exists,
            )

            get_files = PythonOperator(
                task_id="get_files",
                python_callable=DR.get_files,
            )

            extract_all_zip_files = PythonOperator(
                task_id="extract_all_zip_files",
                python_callable=DR.extract_all_zip_files,
            )

            split_files = PythonOperator(
                task_id="split_multiple_file",
                python_callable=DR.split_multiple_files,
            )

            get_way4 = PythonOperator(
                task_id="get_way4_data",
                python_callable=DR.get_way4_data,
                trigger_rule=TriggerRule.ALL_DONE,
            )

            # put_file = PythonOperator(
            #     task_id="put_file",
            #     python_callable=DR.put_file,
            # )

            end = EmptyOperator(
                task_id="END", trigger_rule=TriggerRule.ALL_DONE
            )

            start >> check_chk >> get_files >> extract_all_zip_files >> split_files >> get_way4 >> end
            # start >> check_chk >> get_files >> extract_all_zip_files >> split_files >> get_way4 >> put_file >> end
            # start >> check_chk >> get_files >> extract_all_zip_files >> process_on_us >> split_files >> put_file >> end

    return dag
