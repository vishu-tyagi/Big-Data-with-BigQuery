FROM apache/airflow

ENV AIRFLOW_HOME=/opt/airflow

USER root

# COPY requirements.txt .
# RUN pip install --no-cache-dir -r requirements.txt

SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

WORKDIR $AIRFLOW_HOME

USER $AIRFLOW_UID

RUN pip install gcsfs