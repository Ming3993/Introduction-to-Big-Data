from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from shapely.geometry import Polygon
from pyspark.sql.functions import col, regexp_replace

def is_real_overlap(pair):
    inter = pair[0][1].intersection(pair[1][1])
    return inter.area > 0  # chỉ khi có diện tích thực

spark = SparkSession.builder.appName("ShapeOverlapDetection").getOrCreate()

df = spark.read.parquet("shapes.parquet")

shapes = df.rdd.map(lambda row: (row["shape_id"], Polygon(row["vertices"])))

shape_pairs = shapes.cartesian(shapes)

filtered_pairs = shape_pairs.filter(lambda pair: pair[0][0] < pair[1][0])

overlapping_shapes = filtered_pairs.filter(is_real_overlap)

result_df = overlapping_shapes.map(lambda pair: (pair[0][0], pair[1][0])).toDF(["shape_1", "shape_2"])

# Loại bỏ "Shape_" và chuyển về số nguyên
result_df = result_df.withColumn("shape_1", regexp_replace(col("shape_1"), "Shape_", "").cast("int")) \
                     .withColumn("shape_2", regexp_replace(col("shape_2"), "Shape_", "").cast("int"))

sorted_result = result_df.orderBy(["shape_1", "shape_2"])

sorted_result.write.csv("result.csv", header=True, mode="overwrite")
