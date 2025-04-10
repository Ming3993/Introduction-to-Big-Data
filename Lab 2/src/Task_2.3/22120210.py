from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, regexp_replace
from pyspark.sql.types import ArrayType, IntegerType, BooleanType

spark = SparkSession.builder.appName("ShapeOverlapDetection").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Read data
# df = spark. read.parquet("shapes.parquet") # Using current folder
df = spark.read.parquet("hdfs://localhost:9000/hcmus/lab2/shapes.parquet") # Using HDFS

# Sort vertices
def sortVertices(arr):
    ySorted = sorted(arr, key = lambda pair: pair[1])
    yTop = ySorted[:2]
    yBottom = ySorted[2:]

    yTop = sorted(yTop, key = lambda pair: pair[0])
    yBottom = sorted(yBottom, key = lambda pair: pair[0], reverse=True)

    return yTop + yBottom

sortVerticesUDF = udf(sortVertices, ArrayType(ArrayType(IntegerType())))
sortedDf = df.withColumn("sorted_vertices", sortVerticesUDF("vertices")) \
             .drop("vertices")

# Join
x = sortedDf.alias("x")
y = sortedDf.alias("y")
joinDf = x.crossJoin(y).filter(col("x.shape_id") < col("y.shape_id")) \
                       .select( # Rename
    col("x.shape_id").alias("shape_1"),
    col("y.shape_id").alias("shape_2"),
    col("x.sorted_vertices").alias("vertices_1"),
    col("y.sorted_vertices").alias("vertices_2"),
)

def getLineFromPoints(p1, p2):
    x1, y1 = p1
    x2, y2 = p2

    A = y2 - y1
    B = x1 - x2
    C = x2*y1 - x1*y2

    return (A, B, C)  # Represents line: Ax + By + C = 0

def getPointPairs(polygon):
    for i in range(len(polygon)):
        cur_point = polygon[i]
        next_point = polygon[(i + 1) % len(polygon)]  # wrap around to the start
        yield (cur_point, next_point)

def getPointValue(line, point):
    A, B, C = line
    x, y = point
    return A * x + B * y + C

def getPolygonSide(line, polygon):
    values = [getPointValue(line, point) for point in polygon]

    positive = all(value >= 0 for value in values)
    negative = all(value <= 0 for value in values)

    if positive:
        return 1
    elif negative:
        return -1
    else:
        return 0

# Overlap detection
def detectOverlapping(polygon1, polygon2):
    polygons = [polygon1, polygon2]
    for polygon in polygons:
        for p1, p2 in getPointPairs(polygon):
            line = getLineFromPoints(p1, p2)

            side1 = getPolygonSide(line, polygon1)
            side2 = getPolygonSide(line, polygon2)

            if side1 != 0 and side2 != 0 and side1 != side2:
                return False  # Found a separating axis

    return True

detectOverlappingUDF = udf(detectOverlapping, BooleanType())
resultDf = joinDf.filter(
    detectOverlappingUDF(col("vertices_1"), col("vertices_2"))
)

resultDf = resultDf.drop("vertices_1", "vertices_2") \
                   .withColumn("shape_1", regexp_replace(col("shape_1"), "Shape_", "").cast("int")) \
                   .withColumn("shape_2", regexp_replace(col("shape_2"), "Shape_", "").cast("int"))

# Write data
# resultDf.write.csv("results", header=True, mode="overwrite") # Write on current folder
resultDf.write.csv("hdfs://localhost:9000/hcmus/lab2/results", header=True, mode="overwrite") # Write on HDFS
