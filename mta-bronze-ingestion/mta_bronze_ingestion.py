from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, LongType, IntegerType

schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("route_id", StringType(), True),
    StructField("timestamp", LongType(), True),
    StructField("stop_time_updates", ArrayType(StructType([
        StructField("stop_id", StringType(), True),
        StructField("arrival_delay", IntegerType(), True),
        StructField("arrival_time", LongType(), True)
    ])), True)
])

hudi_options = {
    'hoodie.table.name': 'mta_trips_table',
    'hoodie.table.type': 'MERGE_ON_READ',
    'hoodie.datasource.write.recordkey.field': 'trip_id',
    'hoodie.datasource.write.partitionpath.field': 'route_id',
    'hoodie.datasource.write.precombine.field': 'timestamp',
    'hoodie.datasource.write.operation': 'upsert',
    'hoodie.datasource.write.table.name': 'mta_trips_table',
    'hoodie.datasource.write.table.type': 'MERGE_ON_READ',
    'hoodie.storage.type': 'HADOOP',
    'hoodie.embed.timeline.server': 'false'
}

spark = SparkSession.builder \
    .appName("MTA Bronze Ingestion") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.committer.name", "directory") \
    .config("spark.hadoop.fs.s3a.committer.staging.tmpedir", "/tmp/spark_staging") \
    .config("spark.hadoop.fs.s3a.fast.upload", "true") \
    .config("spark.hadoop.fs.s3a.change.detection.mode", "none") \
    .config("spark.hadoop.fs.s3a.change.detection.version.required", "false") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.metadatastore.impl", "org.apache.hadoop.fs.s3a.s3guard.NullMetadataStore") \
    .getOrCreate()

# Set the log level to DEBUG or ALL
# spark.sparkContext.setLogLevel("DEBUG")

# To specifically target the components causing issues (Kafka/Hudi):
# log4jLogger = spark._jvm.org.apache.log4j
# log4jLogger.LogManager.getLogger("org.apache.kafka").setLevel(log4jLogger.Level.DEBUG)
# log4jLogger.LogManager.getLogger("org.apache.hudi").setLevel(log4jLogger.Level.DEBUG)

## TODO Parametrize Kafka bootstrap servers and topic name
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "mta_live_updates") \
    .option("startingOffsets", "earliest") \
    .load()

parsed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

query = parsed_df.writeStream \
    .format("hudi") \
    .options(**hudi_options) \
    .option("checkpointLocation", "s3a://mta-bronze/checkpoints/") \
    .outputMode("append") \
    .start("s3a://mta-bronze/data/mta_trips")

query.awaitTermination()