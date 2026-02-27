from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, LongType, IntegerType
from config import settings

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

def create_spark_session():
     return SparkSession.builder \
        .appName("MTA Bronze Ingestion") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0,"
                "org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", settings.s3_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", settings.s3_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", settings.s3_secret_key) \
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

def main():
    spark = create_spark_session()

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers) \
        .option("subscribe", settings.kafka_topic) \
        .option("startingOffsets", "earliest") \
        .load()

    parsed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    query = parsed_df.writeStream \
        .format("hudi") \
        .options(**hudi_options) \
        .option("checkpointLocation", settings.checkpoint_location) \
        .outputMode("append") \
        .start(settings.output_path)

    query.awaitTermination()

if __name__ == "__main__":
    main()