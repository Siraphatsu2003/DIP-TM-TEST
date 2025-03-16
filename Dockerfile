FROM apache/airflow:2.9.3-python3.9 AS airflow-base

# Set the AIRFLOW_HOME environment variable
ENV AIRFLOW_HOME=/opt/airflow

# First stage: Builder
FROM airflow-base AS builder

# USER root

COPY requirements.txt /

RUN pip install --no-cache-dir "apache-airflow==2.9.3" --upgrade pip && \
    # pip install --no-cache-dir "apache-airflow==2.9.3" esdk-obs-python==3.24.6 \
    pip install --no-cache-dir "apache-airflow==2.9.3" -r /requirements.txt 
#    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.9.txt"

# Second stage: Final image
# FROM airflow-base

# USER root

# # Copy installed packages from the builder stage
# COPY --from=builder /usr/local/lib/python3.9/site-packages /usr/local/lib/python3.9/site-packages
# COPY --from=builder /usr/local/bin /usr/local/bin

# USER "${AIRFLOW_UID:-50000}"
