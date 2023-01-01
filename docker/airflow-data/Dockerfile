ARG AIRFLOW_BASE_IMAGE="apache/airflow:2.5.0-python3.8"
FROM ${AIRFLOW_BASE_IMAGE}
USER root
RUN mkdir -p /data && chown airflow /data
USER airflow
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
RUN pip install apache-airflow[amazon]==2.5.0 \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.5.0/constraints-3.7.txt"
