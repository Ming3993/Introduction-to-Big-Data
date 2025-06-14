from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, stddev, to_timestamp, to_json, struct, lit, collect_list, date_format, concat_ws
from pyspark.sql.types import StructType, StringType
import os

# Schema dữ liệu đầu vào
spark = SparkSession.builder.appName('BTC-Transform-1').config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
schema = StructType().add("symbol", StringType()).add("price", StringType()).add("timestamp", StringType())

# Đọc dữ liệu từ Kafka topic btc-price
df_raw = spark.readStream.format('kafka').option("kafka.bootstrap.servers", os.getenv("KAFKA_BROKER")).option("subscribe", "btc-price").load()

# Parse JSON và ép kiểu timestamp
df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_str").select(from_json(col("json_str"), schema).alias("data")).select(
    col("data.symbol"),
    col("data.price").cast("double").alias("price"),
    to_timestamp(col("data.timestamp"), "yyyyMMdd'T'HHmmss.SSS'Z'").alias("ts")
)

# Danh sách khung thời gian cần tính moving
windows = [("30s", "30 seconds"), ("1m", "1 minute"), ("5m", "5 minutes"), ("15m", "15 minutes"), ("30m", "30 minutes"), ("1h", "1 hour")]

# Dữ liệu từ nhiều cửa sổ
window_stats = None
for label, duration in windows:
    df_win = df_parsed.withWatermark("ts", "10 seconds").groupBy(
        window(col("ts"), duration),
        col("symbol")
    ).agg(
        avg("price").alias("avg_price"),
        stddev("price").alias("std_price")
    ).select(
        col("symbol"),
        col("window.start").alias("emit_ts"),
        lit(label).alias("window"),
        col("avg_price"),
        col("std_price")
    )
    if window_stats is None:
        window_stats = df_win
    else:
        window_stats = window_stats.union(df_win)

# Gom nhiều cửa sổ vào một JSON duy nhất theo timestamp + symbol
grouped = window_stats.groupBy("symbol", "emit_ts").agg(
    collect_list(
        struct(
            col("window"), 
            col("avg_price"), 
            col("std_price")
        )
    ).alias("windows")
).select(
    to_json(
        struct(
            concat_ws("", date_format(col("emit_ts"), "yyyy-MM-dd'T'HH:mm:ss.SSS"), lit("Z")).alias("timestamp"),
            col("symbol"),
            col("windows")
        )
    ).alias("value")
)

# Ghi ra Kafka topic btc-price-moving
grouped.writeStream.format("kafka").option("kafka.bootstrap.servers", os.getenv("KAFKA_BROKER")).option("topic", "btc-price-moving").option("checkpointLocation", '/tmp/btc-transform-checkpoint').outputMode("update").start().awaitTermination()