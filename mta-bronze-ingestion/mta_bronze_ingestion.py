from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, LongType, IntegerType
from config import settings
from spark_builder import create_spark_session

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

def main():
    spark = create_spark_session("MTA Bronze Ingestion")

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers) \
        .option("subscribe", settings.kafka_topic_name) \
        .option("startingOffsets", "earliest") \
        .load()

    parsed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    query = parsed_df.writeStream \
        .format("hudi") \
        .options(**hudi_options) \
        .option("checkpointLocation", settings.bronze_checkpoint_location) \
        .outputMode("append") \
        .start(settings.bronze_output_path)

    query.awaitTermination()

if __name__ == "__main__":
    main()