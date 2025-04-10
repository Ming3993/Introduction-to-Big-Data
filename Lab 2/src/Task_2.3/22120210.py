from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, regexp_replace
from pyspark.sql.types import ArrayType, IntegerType, BooleanType

# Create session with Spark
spark = SparkSession.builder.appName("ShapeOverlapDetection").getOrCreate()

# Read data
# df = spark. read.parquet("shapes.parquet") # Using the folder storing this Spark program file
df = spark.read.parquet("hdfs://localhost:9000/hcmus/lab2/shapes.parquet") # Using HDFS

# Sort vertices to the order: top-left, top-right, bottom-right, bottom-left
# To ensure two adjacent points create an edge
def sortVertices(arr):
    # Sort by y coordination first
    ySorted = sorted(arr, key = lambda pair: pair[1], reverse=True)

    yTop = ySorted[:2] # Two top points
    yBottom = ySorted[2:] # Two bottom points

    # Then sort by x coordination
    yTop = sorted(yTop, key = lambda pair: pair[0]) # Ascending
    yBottom = sorted(yBottom, key = lambda pair: pair[0], reverse=True) # Descending

    return yTop + yBottom

sortVerticesUDF = udf(sortVertices, ArrayType(ArrayType(IntegerType()))) # Create a UDF
sortedDf = df.withColumn("sorted_vertices", sortVerticesUDF("vertices")) \
             .drop("vertices") # Create new column and discard the unsorted one

# Self join and rename
x = sortedDf.alias("x")
y = sortedDf.alias("y")
joinDf = x.crossJoin(y).filter(col("x.shape_id") < col("y.shape_id")) \
                       .select( # Rename
    col("x.shape_id").alias("shape_1"),
    col("y.shape_id").alias("shape_2"),
    col("x.sorted_vertices").alias("vertices_1"),
    col("y.sorted_vertices").alias("vertices_2"),
) # Filter to avoid duplication

# Overlap detection
def getLineFromPoints(p1, p2): # Get value of A, B, C in the function Ax + By + C
                               # given two points M, N
    x1, y1 = p1
    x2, y2 = p2

    A = y2 - y1
    B = x1 - x2
    C = x2*y1 - x1*y2

    return (A, B, C)

def getPointPairs(polygon): # Get a pair point to form a line
    for i in range(len(polygon)):
        cur_point = polygon[i]
        next_point = polygon[(i + 1) % len(polygon)]  # Wrap around to the start
        yield (cur_point, next_point)

def getPointValue(line, point): # Get value from the function Ax + By + C
    A, B, C = line
    x, y = point
    return A * x + B * y + C

def getPolygonSide(line, polygon): # Check if all the points are in one side of the line
    values = [getPointValue(line, point) for point in polygon]

    positive = all(value >= 0 for value in values)
    negative = all(value <= 0 for value in values)

    if positive: # All the points give positive values
        return 1
    elif negative: # All the points give negative values
        return -1
    else: # All the points are not in the same side
        return 0

def detectOverlapping(polygon1, polygon2): # Given two polygons, check if they are overlapping using SAT
                                           # That is, iterate over all the edge of the two polygons:
                                           # If one of them seperate two polygons to two side, then they are not overlapped
    polygons = [polygon1, polygon2]
    for polygon in polygons:
        for p1, p2 in getPointPairs(polygon):
            line = getLineFromPoints(p1, p2) # Get the line function of one edge

            side1 = getPolygonSide(line, polygon1)
            side2 = getPolygonSide(line, polygon2)

            if side1 != 0 and side2 != 0 and side1 != side2: # If they are seperated by that line
                return False  # Found a separating axis -> they are not overlapped

    return True # They are overlapped

detectOverlappingUDF = udf(detectOverlapping, BooleanType()) # Set the UDF function
resultDf = joinDf.filter(
    detectOverlappingUDF(col("vertices_1"), col("vertices_2"))
)

resultDf = resultDf.drop("vertices_1", "vertices_2") \
                   .withColumn("shape_1", regexp_replace(col("shape_1"), "Shape_", "").cast("int")) \
                   .withColumn("shape_2", regexp_replace(col("shape_2"), "Shape_", "").cast("int")) # Modify the result format

# Write data
# resultDf.write.csv("results", header=True, mode="overwrite") # Write to the folder storing this Spark program file
resultDf.write.csv("hdfs://localhost:9000/hcmus/lab2/results", header=True, mode="overwrite") # Write to HDFS
