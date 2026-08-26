.PHONY: setup init up scheduler api-server test-dag dags clean

AIRFLOW := AIRFLOW_HOME=$(CURDIR) $(CURDIR)/.venv/bin/airflow

setup:
	bash scripts/setup.sh

init:
	$(AIRFLOW) db migrate

up:
	$(AIRFLOW) standalone

scheduler:
	$(AIRFLOW) scheduler

api-server:
	$(AIRFLOW) api-server --port 8080

test-dag:
	$(AIRFLOW) dags test example_sales_etl

dags:
	$(AIRFLOW) dags list

clean:
	@echo "Untuk reset lokal, hapus airflow.db dan logs secara manual."
