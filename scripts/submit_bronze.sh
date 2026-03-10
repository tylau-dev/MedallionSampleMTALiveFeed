set -eu

MASTER_URL=${SPARK_MASTER_URL:-spark://spark-master:7077}
PACKAGES=${SPARK_PACKAGES:-org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\
org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0,\
org.apache.hadoop:hadoop-aws:3.3.4}

/opt/spark/bin/spark-submit \
    --master "$MASTER_URL" \
    --conf "spark.driver.extraJavaOptions=-Divy.home=/tmp/.ivy2" \
    --conf "spark.executor.extraJavaOptions=-Divy.home=/tmp/.ivy2" \
    --packages "$PACKAGES" \
    --py-files src/common/ \
    /opt/spark/work-dir/src/jobs/bronze/mta_bronze.py "$@"