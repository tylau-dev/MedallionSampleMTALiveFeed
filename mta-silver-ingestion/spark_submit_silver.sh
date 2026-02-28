set -eu -o pipefail

if [ -f "$(dirname "$0")/../.env" ]; then
    export $(grep -v '^\s*#' "$(dirname "$0")/../.env" | xargs)
fi

MASTER_URL=${SPARK_MASTER_URL:-spark://spark-master:7077}
PACKAGES=${SPARK_PACKAGES:-org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\
org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0,\
org.apache.hadoop:hadoop-aws:3.3.4}

docker exec -i spark-master /opt/spark/bin/spark-submit \
    --master "$MASTER_URL" \
    --conf "spark.driver.extraJavaOptions=-Divy.home=/tmp/.ivy2" \
    --conf "spark.executor.extraJavaOptions=-Divy.home=/tmp/.ivy2" \
    --packages "$PACKAGES" \
    /opt/spark/work-dir/mta-silver-ingestion/mta_silver_ingestion.py "$@"