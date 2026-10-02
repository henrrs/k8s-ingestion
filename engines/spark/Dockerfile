FROM apache/spark:4.2.0-python3@sha256:fc64959c04bd87b0ac686be9aaa9008b69cdb1afc695400528279c8b01f43d89
USER root
RUN apt-get update \
    && apt-get install --no-install-recommends -y \
        libgssapi-krb5-2 \
        libkrb5-3 \
        libltdl7 \
    && rm -rf /var/lib/apt/lists/*
COPY engines/spark/dependencies/jars/ /opt/spark/jars/
COPY engines/spark/launcher.py /opt/company/launcher.py
COPY dist/company_ingestion_core-0.2.0-py3-none-any.whl /tmp/company_ingestion_core-0.2.0-py3-none-any.whl
RUN python3 -m pip install --no-cache-dir --no-deps \
        delta-spark==4.4.0 \
        pyarrow==25.0.1 \
    && python3 -m pip install --no-cache-dir \
        numpy==2.2.6 \
        pandas==2.3.3 \
        mssql-python==1.15.0 \
    && python3 -m pip install --no-cache-dir --no-deps /tmp/company_ingestion_core-0.2.0-py3-none-any.whl \
    && rm /tmp/company_ingestion_core-0.2.0-py3-none-any.whl \
    && chmod -R a+rX /opt/company
ENV COMPANY_RUNTIME_VERSION=0.2.0
USER 185
