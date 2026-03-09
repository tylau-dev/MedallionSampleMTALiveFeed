from pyspark.sql.functions import approx_count_distinct, avg, count_distinct, current_timestamp, date_format, dayofmonth, explode, col, from_unixtime, month, to_timestamp, window, year
from shared.config import settings
from shared.spark_builder import create_spark_session

hudi_options = {
    'hoodie.table.name': 'mta_trips_gold',
    'hoodie.table.type': 'COPY_ON_WRITE',
    'hoodie.datasource.write.recordkey.field': 'route_id,window_start',
    'hoodie.datasource.write.partitionpath.field': 'year,month,day',
    'hoodie.datasource.write.precombine.field': 'last_update',
    'hoodie.datasource.write.operation': 'upsert',
    'hoodie.datasource.write.table.name': 'mta_trips_gold',
    'hoodie.datasource.write.table.type': 'COPY_ON_WRITE',
    'hoodie.storage.type': 'HADOOP',
    'hoodie.embed.timeline.server': 'false'
}

def main():
    spark = create_spark_session("MTA Gold Data Aggregation")

    silver_df = spark.readStream \
        .format("hudi") \
        .load(settings.silver_output_path)

    silver_with_watermark = silver_df \
        .withColumn("event_time", to_timestamp(col("processing_timestamp"))) \
        .withColumn("stop_arrival_timestamp", from_unixtime(col("stop_arrival_time"))) \
        .withWatermark("event_time", "10 minutes")

    gold_df = silver_with_watermark \
        .groupBy(
            "route_id",
            window(col("stop_arrival_timestamp"), "10 minutes").alias("time_window")
        ).agg(
            approx_count_distinct("trip_id", 0.05).alias("total_trips"),
            avg("arrival_delay").alias("avg_arrival_delay")
        ).select(
            col("route_id"),
            col("time_window.start").alias("window_start"),
            year(col("time_window.start")).alias("year"),
            month(col("time_window.start")).alias("month"),
            dayofmonth(col("time_window.start")).alias("day"),
            date_format(col("time_window.start"), "HH:mm").alias("time_slice"),
            "total_trips",
            "avg_arrival_delay",
            current_timestamp().alias("last_update")
        )

    query = gold_df.writeStream \
        .format("hudi") \
        .options(**hudi_options) \
        .trigger(availableNow=True) \
        .option("checkpointLocation", settings.gold_checkpoint_location) \
        .outputMode("complete") \
        .start(settings.gold_output_path)

    query.awaitTermination()

if __name__ == "__main__":
    main()