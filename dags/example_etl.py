"""Contoh pipeline ETL sederhana menggunakan TaskFlow API Airflow 3."""

from __future__ import annotations

import csv
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import dag, task


AIRFLOW_HOME = Path(os.environ.get("AIRFLOW_HOME", Path(__file__).parents[1]))
DATA_DIR = AIRFLOW_HOME / "data"


@dag(
    dag_id="example_sales_etl",
    description="Mengekstrak, mentransformasi, dan menyimpan ringkasan penjualan",
    schedule="0 7 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["example", "etl"],
    default_args={
        "owner": "data-team",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
)
def sales_etl():
    @task
    def extract() -> list[dict]:
        return [
            {"product": "keyboard", "quantity": 2, "price": 750_000},
            {"product": "mouse", "quantity": 3, "price": 250_000},
            {"product": "monitor", "quantity": 1, "price": 2_500_000},
        ]

    @task
    def transform(rows: list[dict]) -> list[dict]:
        return [
            {**row, "total": row["quantity"] * row["price"]}
            for row in rows
        ]

    @task
    def load(rows: list[dict]) -> str:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        output = DATA_DIR / "sales_summary.csv"
        with output.open("w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(
                file, fieldnames=["product", "quantity", "price", "total"]
            )
            writer.writeheader()
            writer.writerows(rows)
        return str(output)

    load(transform(extract()))


sales_etl()
