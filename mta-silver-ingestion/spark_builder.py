from pyspark.sql import SparkSession
from config import settings

def create_spark_session(app_name: str):
     return SparkSession.builder \
        .appName(app_name) \
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