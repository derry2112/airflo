FROM apache/airflow:3.1.7-python3.12
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir "apache-airflow==3.1.7" \
    -r /tmp/requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.1.7/constraints-3.12.txt"
