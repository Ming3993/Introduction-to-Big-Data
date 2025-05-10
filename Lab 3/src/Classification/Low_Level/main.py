from classification.trainer import LogisticRegression
from classification.evaluator import BinaryClassificationEvaluator
from classification.preprocessor import ZScoreNormalization
from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder \
        .appName("Logistic Regression Low Level") \
        .master("local[*]") \
        .getOrCreate()

# Set the log level to WARN to reduce verbosity
spark.sparkContext.setLogLevel("WARN")

# Load the data
rdd = spark.sparkContext.textFile(f"hdfs://localhost:9000/creditcard.csv")  # Load the data from HDFS

# Format the data
header = rdd.first()  # Extract the header
rdd_format = (
    rdd.filter(lambda line: line != header)  # Remove the header
       .map(lambda line: line.split(","))  # Split each line by comma
       .map(lambda line: line[1:])  # Remove the time column as it is just for time note
       .map(lambda line: [float(x.strip('"')) if isinstance(x, str) else float(x) for x in line])  # Convert to float 
       .map(lambda line: [line[:-1] + [1.0], line[-1]])  # Separate features and label
)  

# Split the data into training and testing sets
train_rdd, test_rdd = rdd_format.randomSplit([0.8, 0.2], seed=42)

# Normalize the training data
normalizer = ZScoreNormalization(column_index=0)  
train_rdd_normalized = normalizer.fit(train_rdd).transform(train_rdd)  # Transform the training data, the column for normalization is now in 2

# Train the model
learning_rate = 0.001
num_iterations = 200
batch_size = 2048
threshold = 0.5
regression = LogisticRegression(learning_rate=learning_rate, 
                                num_iterations=num_iterations, 
                                threshold=threshold,
                                batch_size=batch_size,
                                feature_index_train=2,
                                feature_index_test=0,
                                label_index=1)
model = regression.train(train_rdd_normalized)  # Train model

# Evaluate the model
test_rdd_predictions = model.predict(test_rdd) # Make predictions, column of predicted labels is added to the end of the row, which is 2
evaluator = BinaryClassificationEvaluator(true_label_col=1, predicted_label_col=2)
result = evaluator.evaluate(test_rdd_predictions)  

print(f"Accuracy: {result['accuracy']}")
print(f"Precision: {result['precision']}")
print(f"Recall: {result['recall']}")
print(f"F1 Score: {result['f1_score']}")

# Write out the predictions to file system
output_name = "classification_results"
output_header = "V1,V2,V3,V4,V5,V6,V7,V8,V9,V10,V11,V12,V13,V14,V15,V16,V17,V18,V19,V20,V21,V22,V23,V24,V25,V26,V27,V28,Amount,True Label,Prediction Label"
formatted_output_rdd = test_rdd_predictions.map(lambda x: x[0][:-1] + x[1:]) \
                                           .map(lambda x: ",".join([str(i) for i in x])) \
                                           .map(lambda x: x.replace("[", "").replace("]", "").replace("'", ""))  # Format the output
final_output_rdd = spark.sparkContext.parallelize([output_header]).union(formatted_output_rdd)
final_output_rdd.saveAsTextFile(f"{output_name}")

# Stop the Spark session
spark.stop()