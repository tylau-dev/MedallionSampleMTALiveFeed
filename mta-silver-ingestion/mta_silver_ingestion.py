from pyspark.sql.functions import current_timestamp, explode, col, from_unixtime
from config import settings
from spark_builder import create_spark_session

hudi_options = {
    'hoodie.table.name': 'mta_trips_silver',
    'hoodie.table.type': 'COPY_ON_WRITE',
    'hoodie.datasource.write.recordkey.field': 'trip_id,stop_id',
    'hoodie.datasource.write.partitionpath.field': 'route_id',
    'hoodie.datasource.write.precombine.field': 'event_time',
    'hoodie.datasource.write.operation': 'upsert',
    'hoodie.datasource.write.table.name': 'mta_trips_silver',
    'hoodie.datasource.write.table.type': 'COPY_ON_WRITE',
    'hoodie.storage.type': 'HADOOP',
    'hoodie.embed.timeline.server': 'false'
}

def main():
    spark = create_spark_session("MTA Silver Data Cleaning")

    bronze_df = spark.readStream \
        .format("hudi") \
        .load(settings.bronze_output_path)

    silver_df = bronze_df \
        .filter(col("trip_id").isNotNull()) \
        .withColumn("event_time", from_unixtime(col("timestamp"))) \
        .withColumn("processing_timestamp", current_timestamp()) \
        .withColumn("stop_update", explode(col("stop_time_updates"))) \
        .select(
            "trip_id",
            "route_id",
            "event_time",
            col("stop_update.stop_id").alias("stop_id"),
            col("stop_update.arrival_delay").alias("arrival_delay"),
            col("stop_update.arrival_time").alias("stop_arrival_time"),
            "processing_timestamp"
        )

    silver_df = bronze_df.filter(col("trip_id").isNotNull()) \
        .withColumn("event_time", from_unixtime(col("timestamp"))) \
        .withColumn("processing_timestamp", current_timestamp()) \

    query = silver_df.writeStream \
        .format("hudi") \
        .options(**hudi_options) \
        .option("checkpointLocation", settings.silver_checkpoint_location) \
        .outputMode("append") \
        .start(settings.silver_output_path)

    query.awaitTermination()

if __name__ == "__main__":
    main()