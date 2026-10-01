FROM apache/airflow:2.9.3

USER root
RUN apt-get update && apt-get install -y --no-install-recommends gcc --fix-missing \
    && rm -rf /var/lib/apt/lists/* || true

COPY rds-ca-bundle.pem /home/airflow/rds-ca-bundle.pem
RUN chmod 644 /home/airflow/rds-ca-bundle.pem

USER airflow

COPY requirements-pipeline.txt /requirements-pipeline.txt
RUN pip install --no-cache-dir -r /requirements-pipeline.txt
