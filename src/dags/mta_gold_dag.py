from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    'mta_gold_tranformation',
    start_date=datetime.now(),
    schedule_interval='@hourly',
    catchup=True
) as dag:
    run_gold_aggregation = SparkSubmitOperator(
        task_id='run_gold_aggregation',
        conn_id='spark_default',
        application='/opt/airflow/jobs/gold/mta_gold_ingestion.py',
        name='mta_gold_aggregation',
        deploy_mode='client',
        env_vars={
            "S3_ENDPOINT": "http://minio:9000",
            "S3_ACCESS_KEY": "admin",
            "S3_SECRET_KEY": "password"
            },
        packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0," \
            "org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0," \
            "org.apache.hadoop:hadoop-aws:3.3.4",
        conf={
            "spark.master": "spark://spark-master:7077",
            "spark.driver.host": "airflow",
            "spark.submit.deployMode": "client",
            "spark.driver.extraJavaOptions": "-Divy.home=/tmp/.ivy2",
            "spark.executor.extraJavaOptions": "-Divy.home=/tmp/.ivy2"
        }
    )

    notify_completion = BashOperator(
        task_id='notify_completion',
        bash_command='echo "Gold aggregation completed at $(date)"')

    run_gold_aggregation >> notify_completion