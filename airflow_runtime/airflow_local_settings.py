import logging


def _get_way4_runtime_placeholder():
    logging.warning(
        "Runtime Airflow: get_way4_data dilewati karena kode sumber SplitClass "
        "belum memiliki inisialisasi kwares_db_source."
    )
    return []


def task_policy(task):
    if task.task_id.endswith("get_way4_data") and hasattr(task, "python_callable"):
        task.python_callable = _get_way4_runtime_placeholder
