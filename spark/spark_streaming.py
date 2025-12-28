from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, expr, when,
    current_timestamp, avg, min, max, stddev, sum, count
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, BooleanType, DoubleType
)

# SparkSession
spark = (
    SparkSession.builder
    .appName("Binance-Kafka-Spark-Streaming")
    .config("spark.sql.caseSensitive", "true") # because we have 'e' and 'E', and 't' and 'T' which spark by default treats the same.
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN") # added the warn level to spot the warnings, errors, etc.

# Java database connection
JDBC_URL = "jdbc:postgresql://postgres:5432/crypto"
JDBC_PROPS = {
    "user": "spark",
    "password": "krypto", # hehe
    "driver": "org.postgresql.Driver"
}

# Defining schema
binance_schema = StructType([
    StructField("stream", StringType()),
    StructField("data", StructType([
        StructField("e", StringType()),
        StructField("E", LongType()), # epoch time
        StructField("s", StringType()),
        StructField("t", LongType()),
        StructField("p", StringType()), # parsing failed when tried double because the json value is str 
        StructField("q", StringType()), # same as above. and thats why cast later
        StructField("T", LongType()), # epoch time
        StructField("m", BooleanType()),
        StructField("M", BooleanType()),
        StructField("ingestion_time", LongType()) # ingestion time set during handle_trade, epoch time
    ]))
])

# Reading from kafka
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "binance_trades")
    .option("startingOffsets", "latest")
    .load()
)


# parsing, renaming, and flattening incoming kafka json
parsed_df = (
    kafka_df
    .selectExpr("CAST(value AS STRING) AS json_str")
    .select(from_json(col("json_str"), binance_schema).alias("json"))
    .select(
        col("json.data.e").alias("event_type"),
        col("json.data.E").alias("event_time_ms"),
        col("json.data.s").alias("symbol"),
        col("json.data.t").alias("trade_id"),
        col("json.data.p").alias("price_str"),
        col("json.data.q").alias("quantity_str"),
        col("json.data.T").alias("trade_time_ms"),
        col("json.data.m").alias("is_buyer_maker"),
        col("json.data.ingestion_time").alias("ingestion_time_ms")
    )
)

# casting as double and timestamp. binance uses epoch and so does our producer.
better_df = (
    parsed_df
    .withColumn("price", col("price_str").cast(DoubleType())) # convert string to doubletype as mentioned earlier
    .withColumn("quantity", col("quantity_str").cast(DoubleType())) # same as above
    .withColumn("event_time", (col("event_time_ms") / 1000).cast("timestamp")) # event_time in seconds, event_time_ms in milliseconds epoch
    .withColumn("trade_time", (col("trade_time_ms") / 1000).cast("timestamp")) # trade_time in seconds, trade_time_ms in milliseconds epoch
    .withColumn("ingestion_time", (col("ingestion_time_ms") / 1000).cast("timestamp")) # ingestion_time in seconds, ingestion_time_ms in milliseconds epoch
    .withColumn("spark_time", current_timestamp()) # spark processing timestamp, epoch milliseconds
)

# calculating latency. since we use epoch, the latency is in milliseconds
latency_df = (
    better_df
    .withColumn(
        "exchange_latency_ms",
        (col("ingestion_time_ms") - col("trade_time_ms")).cast(DoubleType()) # in milliseconds
    )
    .withColumn(
        "source_latency_ms",
        (col("ingestion_time_ms") - col("event_time_ms")).cast(DoubleType()) # in milliseconds
    )
    .withColumn(
        "end_to_end_latency_ms",
        (col("spark_time").cast("double") * 1000 - col("ingestion_time_ms")).cast(DoubleType()) # used unix_timestamp earlier caused the loss of miliseconds which caused the latency calculations to be skewed
    )
)

# writing to raw_trades table
def write_raw_trades(batch_df, batch_id):
    (
        batch_df
        .select(
            "symbol",
            "trade_id",
            "price",
            "quantity",
            "is_buyer_maker",
            "event_time",
            "trade_time",
            "ingestion_time",
            "spark_time",
            "exchange_latency_ms",
            "source_latency_ms",
            "end_to_end_latency_ms"
        )
        .write
        .jdbc(JDBC_URL, "raw_trades", "append", JDBC_PROPS)
    )

raw_query = (
    latency_df
    .writeStream
    .foreachBatch(write_raw_trades)
    .outputMode("append")
    .start()
)

# metrics calculation for every 5 second window
metrics_df = (
    latency_df
    .withWatermark("event_time", "30 seconds")
    .groupBy(
        window(col("event_time"), "5 seconds"),
        col("symbol")
    )
    .agg(
        count("*").alias("trade_count"),
        sum("quantity").alias("total_volume"),
        avg("price").alias("avg_price"),
        min("price").alias("min_price"),
        max("price").alias("max_price"),
        stddev("price").alias("price_stddev"),
        (sum(col("price") * col("quantity")) / sum("quantity")).alias("vwap"),
        sum(when(col("is_buyer_maker") == False, col("quantity")).otherwise(0)).alias("buy_volume"),
        sum(when(col("is_buyer_maker") == True, col("quantity")).otherwise(0)).alias("sell_volume"),
        avg("exchange_latency_ms").alias("avg_exchange_latency_ms"),
        avg("source_latency_ms").alias("avg_source_latency_ms"),
        avg("end_to_end_latency_ms").alias("avg_end_to_end_latency_ms")
    )
    .withColumn(
        "buy_sell_ratio",
        when(col("sell_volume") != 0, col("buy_volume") / col("sell_volume"))
    )
    .withColumn("trade_intensity", col("trade_count") / expr("5")) # because we set event_time window at 5 seconds earlier so dividing with 5 gives trade per second
    .withColumn("liquidity_proxy", col("total_volume") / expr("5")) 
    .select(
        col("symbol"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "*"
    ) 

)

# writing metrics to postgres
def write_metrics(batch_df, batch_id): # added batch id for future when i work on refining the pipeline.
    (
        batch_df
        .drop("window")
        .write
        .jdbc(JDBC_URL, "metrics", "append", JDBC_PROPS)
    )

metrics_query = (
    metrics_df
    .writeStream
    .foreachBatch(write_metrics)
    .outputMode("append")
    .start()
)

spark.streams.awaitAnyTermination()
