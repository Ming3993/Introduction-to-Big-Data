from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("LoadStage") \
    .config("spark.mongodb.write.connection.uri", "mongodb://mongodb:27017") \
    .config("spark.mongodb.write.database", "crypto") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schema của Z-score từ Kafka
zscore_schema = StructType([
    StructField("timestamp", StringType()),
    StructField("symbol", StringType()),
    StructField("zscores", ArrayType(
        StructType([
            StructField("window", StringType()),
            StructField("zscore_price", DoubleType())
        ])
    ))
])

# Đọc dữ liệu từ Kafka
df_raw = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("subscribe", "btc-price-zscore") \
    .option("startingOffsets", "latest") \
    .load()

df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), zscore_schema).alias("data")) \
    .select("data.*")

# Xử lý dữ liệu theo từng cửa sổ và ghi vào MongoDB
def write_to_mongo(window):
    return df_parsed.select(
        col("timestamp"),
        col("symbol"),
        col("zscores")
    ).withColumn("zscores", 
        col("zscores")
    ).filter(
        f"array_contains(transform(zscores, x -> x.window), '{window}')"
    ).writeStream \
    .format("mongodb") \
    .option("checkpointLocation", f"/tmp/checkpoints/{window}") \
    .option("collection", f"btc-price-zscore-{window}") \
    .outputMode("append") \
    .start()

queries = [
    write_to_mongo("30s"),
    write_to_mongo("1m"),
    write_to_mongo("5m"),
    write_to_mongo("15m"),
    write_to_mongo("30m"),
    write_to_mongo("1h")
]

for q in queries:
    q.awaitTermination()
