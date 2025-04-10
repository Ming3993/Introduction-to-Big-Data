from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, to_date, date_sub, sum, min, max, sequence, explode, expr
from datetime import timedelta
import os
import shutil

# Create a SparkSession
spark = SparkSession.builder.appName("Lab2_Task2.2").getOrCreate()

# Read data from CSV with header
df = spark.read.option("header", "true").csv("Datasets/asr.csv")

# Filter rows where 'Status' contains 'shipped' (case-insensitive)
df = df.filter(lower(col("Status")).contains("shipped"))

# Convert "Date" column from string to DateType using correct format
df = df.withColumn("Date", to_date(col("Date"), "MM-dd-yy"))

# Cast "Qty" column to IntegerType
df = df.withColumn("Qty", col("Qty").cast("int"))

# Get min and max date in dataset
date_range = df.select(min("Date").alias("min_date"), max("Date").alias("max_date")).first()
start_date = date_range["min_date"]
end_date = date_range["max_date"] + timedelta(days=7)

# Get first Monday on or after min_date
days_to_add = (8 - start_date.isoweekday()) % 7
first_monday = start_date + timedelta(days=days_to_add)

# Create DateFrame with start and end
date_df = spark.createDataFrame([(first_monday, end_date)], ["start", "end"])

# Generate sequence of Mondays
mondays_df = date_df.select(
    explode(sequence(col("start"), col("end"), expr("INTERVAL 7 DAYS"))).alias("report_date")
)

# Join each Monday with sales in the previous 7 days (excluding Monday itself)
df_window = mondays_df.alias("monday").join(
    df.alias("sale"),
    (col("sale.Date") < col("monday.report_date")) & 
    (col("sale.Date") >= date_sub(col("monday.report_date"), 7))
)

# Group by report date and SKU, then calculate total quantity sold in the 7-day window
result = df_window.groupBy(
    col("monday.report_date"),
    col("sale.SKU").alias("sku")
).agg(
    sum("sale.Qty").alias("total_quantity")
).orderBy("report_date", "sku")

# Write the result to a temporary folder as a single CSV file
result.coalesce(1).write.mode("overwrite").option("header", True).csv("temp_output")

# Rename the generated part-* file to output.csv
for filename in os.listdir("temp_output"):
    if filename.startswith("part-") and filename.endswith(".csv"):
        os.rename(f"temp_output/{filename}", "src/Task_2.2/output.csv")
        break

# Clean up temporary directory
shutil.rmtree("temp_output")