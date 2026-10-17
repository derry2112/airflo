FROM apache/airflow:3.1.7-python3.12
COPY requirements.txt /tmp/requirements.txt
COPY pkgs/ /tmp/pkgs/
RUN pip install --no-cache-dir --no-index --find-links=/tmp/pkgs \
    --constraint /tmp/pkgs/requirements.lock.txt \
    --constraint /tmp/pkgs/constraints-3.12.txt \
    "apache-airflow==3.1.7" -r /tmp/requirements.txt
