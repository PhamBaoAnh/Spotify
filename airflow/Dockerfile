FROM apache/airflow:2.7.3-python3.10

ENV AIRFLOW_HOME=/opt/airflow

# --- System dependencies ---
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        git \
        build-essential \
        curl \
    && rm -rf /var/lib/apt/lists/*

# --- Python dependencies ---
COPY requirements.txt /requirements.txt

# Chuyển sang user airflow để cài pip
USER ${AIRFLOW_UID}
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /requirements.txt

# Ref: https://airflow.apache.org/docs/docker-stack/recipes.html
SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

WORKDIR $AIRFLOW_HOME

USER ${AIRFLOW_UID}
