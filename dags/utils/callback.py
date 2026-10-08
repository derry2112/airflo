"""Local alert callbacks for acquiring DAGs."""

import logging
from html import escape

logger = logging.getLogger(__name__)


class Callback:
    def __init__(self, email_list=None):
        self.email_list = list(email_list or [])

    def _alert(self, context, event):
        ti = context.get("task_instance") or context.get("ti")
        dag_run = context.get("dag_run")
        dag = context.get("dag")
        dag_id = getattr(dag_run, "dag_id", None) or getattr(dag, "dag_id", "unknown")
        task_id = getattr(ti, "task_id", None)
        run_id = getattr(dag_run, "run_id", None) or getattr(ti, "run_id", None)
        subject = f"[Airflow] {event}: {dag_id}"
        details = {"dag_id": dag_id, "run_id": run_id}
        if event == "Task failed":
            details.update(task_id=task_id, exception=str(context.get("exception", "")))
            logger.error("%s | %s", subject, details)
        else:
            logger.info("%s | %s", subject, details)
        if self.email_list:
            from airflow.utils.email import send_email

            body = "<br>".join(f"{escape(key)}: {escape(str(value))}" for key, value in details.items())
            send_email(to=self.email_list, subject=subject, html_content=body)

    def task_failure_alert(self, context):
        self._alert(context, "Task failed")

    def dag_success_alert(self, context):
        self._alert(context, "DAG succeeded")
