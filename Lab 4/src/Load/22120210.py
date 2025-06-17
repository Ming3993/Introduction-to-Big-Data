from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, DoubleType
from pymongo import MongoClient
import json

spark = SparkSession.builder \
    .appName("BTC Z-Score Loader") \
    .master("local[*]") \
    .config("spark.mongodb.write.connection.uri", "mongodb://mongodb:27017/") \
    .getOrCreate()

schema = StructType([
    StructField("timestamp", StringType()), 
    StructField("symbol", StringType()),
    StructField("windows", ArrayType(
        StructType([
            StructField("window", StringType()),          
            StructField("zscore_price", DoubleType())
        ])
    ))
])

df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("subscribe", "btc-price-zscore") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .option("maxOffsetsPerTrigger", "1000") \
    .load()

df_parsed = df_kafka.selectExpr("CAST(value AS STRING) AS json_value") \
    .select(from_json(col("json_value"), schema).alias("data")) \
    .select("data.*")

df_exploded = df_parsed \
    .withColumn("timestamp", to_timestamp(col("timestamp"))) \
    .withColumn("window_data", explode(col("windows"))) \
    .select(
        "timestamp",
        "symbol",
        col("window_data.window").alias("window"),
        col("window_data.zscore_price").alias("zscore_price")
    )

def write_to_windowed_collections(df, epoch_id):
    if df.rdd.isEmpty():
        return

    df_json = df.toJSON().collect()
    
    client = MongoClient("mongodb://mongodb:27017/")
    db = client["btc"]

    for record in df_json:
        try:
            doc = json.loads(record)
            window = doc.get("window")
            if not window:
                continue
            collection_name = f"btc-price-zscore-{window}"
            db[collection_name].insert_one({
                "timestamp": doc["timestamp"],
                "symbol": doc["symbol"],
                "zscore_price": doc["zscore_price"]
            })
        except Exception as e:
            print(f"Error saving document: {e}")

df_exploded.writeStream \
    .foreachBatch(write_to_windowed_collections) \
    .option("checkpointLocation", "/tmp/checkpoints/zscore") \
    .start() \
    .awaitTermination()