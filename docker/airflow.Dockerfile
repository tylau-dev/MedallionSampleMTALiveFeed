FROM apache/spark:3.5.0 AS spark-base

FROM apache/airflow:2.11.1

USER root
RUN apt-get update && apt-get install -y default-jre

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64

COPY --from=spark-base /opt/spark /opt/spark
ENV SPARK_HOME=/opt/spark
ENV PATH="$SPARK_HOME/bin:$PATH"

WORKDIR /opt/airflow

COPY pyproject.toml .
COPY ./src/dags ./src/dags
COPY ./src/jobs/gold ./jobs/gold
COPY ./src/shared ./src/shared

RUN chown -R 50000:0 /opt/airflow
USER airflow

RUN pip install --no-cache-dir ".[airflow]"
RUN pip install --no-cache-dir -e .