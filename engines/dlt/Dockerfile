FROM python:3.12.12-slim@sha256:f3fa41d74a768c2fce8016b98c191ae8c1bacd8f1152870a3f9f87d350920b7c

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DLT_TELEMETRY=false \
    DLT_DATA_DIR=/tmp/dlt

WORKDIR /app
RUN apt-get update \
    && apt-get install --no-install-recommends -y \
        libgssapi-krb5-2 \
        libkrb5-3 \
        libltdl7 \
    && rm -rf /var/lib/apt/lists/*
COPY engines/dlt/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
COPY libraries/ingestion-core /tmp/company-ingestion-core
RUN pip install --no-cache-dir --no-deps /tmp/company-ingestion-core \
    && rm -rf /tmp/company-ingestion-core
COPY engines/dlt /tmp/company-ingestion
RUN pip install --no-cache-dir --no-deps /tmp/company-ingestion && rm -rf /tmp/company-ingestion
COPY engines/dlt/legacy/single_pod.py /app/main.py

USER 10001
ENTRYPOINT ["python", "/app/main.py"]
