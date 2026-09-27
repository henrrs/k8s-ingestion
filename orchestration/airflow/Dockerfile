FROM apache/airflow:3.3.2-python3.12
USER airflow
RUN pip install --no-cache-dir \
    apache-airflow-providers-cncf-kubernetes==10.22.0 \
    kubernetes==36.0.3
COPY --chown=airflow:root orchestration/airflow/provider /opt/company-provider
RUN pip install --no-cache-dir --no-deps /opt/company-provider
COPY --chown=airflow:root orchestration/airflow/dags /opt/airflow/dags
