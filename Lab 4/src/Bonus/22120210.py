from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, to_timestamp, min, col, from_json, when, date_format
from pyspark.sql.types import StructType, StructField, StringType, FloatType

# Spark session
spark = SparkSession.builder \
    .appName("Bonus") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Schema for Kafka message value
schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", StringType(), True),
    StructField("timestamp", StringType(), True)
])

# Read from Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("subscribe", "btc-price") \
    .option("startingOffsets", "latest") \
    .load()

# Parse the Kafka value as JSON and convert timestamp and price types
df = df_raw.selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", to_timestamp("timestamp", "yyyy-MM-dd'T'HH:mm:ss.SSSX")) \
    .withColumn("price", col("price").cast(FloatType())) \
    .withWatermark("timestamp", "30 seconds")

# Base windows per record (t, t + 20 seconds]
df_window = df.selectExpr(
    "timestamp as start_timestamp",
    "timestamp + interval 20 seconds as end_timestamp",
    "price as base_price",
    "symbol"
).dropDuplicates(["symbol", "start_timestamp"]).alias("window")

# Value stream with key by alias
df_value = df.alias("value")

# Join on time range
df_join = df_window.join(
    df_value,
    on="symbol",
    how="inner"
).where(
    (col("value.timestamp") > col("window.start_timestamp")) &
    (col("value.timestamp") <= col("window.end_timestamp"))
)

# Flag records with greater and lesser prices
df_with_flags = df_join \
    .withColumn("greater_ts", when(col("value.price") > col("window.base_price"), col("value.timestamp"))) \
    .withColumn("lesser_ts", when(col("value.price") < col("window.base_price"), col("value.timestamp")))

# Aggregate to find the first timestamps for each window
df_greater = df_with_flags.groupBy("window.start_timestamp") \
    .agg(min("greater_ts").alias("first_greater_ts"))

df_lesser = df_with_flags.groupBy("window.start_timestamp") \
    .agg(min("lesser_ts").alias("first_lesser_ts"))

# Join and compute result
df_final = df_window \
    .join(df_greater, on="start_timestamp", how="left") \
    .join(df_lesser, on="start_timestamp", how="left") \
    .withColumn("higher_window", expr("IF(first_greater_ts IS NULL, 20.0, (CAST(first_greater_ts AS DOUBLE) - CAST(start_timestamp AS DOUBLE)))")) \
    .withColumn("lower_window", expr("IF(first_lesser_ts IS NULL, 20.0, (CAST(first_lesser_ts AS DOUBLE) - CAST(start_timestamp AS DOUBLE)))")) \
    .withColumn("timestamp", date_format(col("start_timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))

# Prepare for topics
df_higher = df_final.selectExpr(
    "CAST(timestamp AS STRING) AS key",
    """to_json(named_struct(
        'timestamp', timestamp,
        'higher_window', higher_window
    )) AS value"""
)

df_lower = df_final.selectExpr(
    "CAST(timestamp AS STRING) AS key",
    """to_json(named_struct(
        'timestamp', timestamp,
        'lower_window', lower_window
    )) AS value"""
)

# Write to topics
query_higher = df_higher.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("topic", "btc-price-higher") \
    .option("checkpointLocation", "/tmp/kafka-checkpoint-higher") \
    .outputMode("append") \
    .start()

# Write to topics
query_lower = df_lower.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("topic", "btc-price-lower") \
    .option("checkpointLocation", "/tmp/kafka-checkpoint-lower") \
    .outputMode("append") \
    .start()

# Wait for both queries
query_higher.awaitTermination()
query_lower.awaitTermination()