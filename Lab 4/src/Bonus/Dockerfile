FROM base-spark:latest

WORKDIR /app
COPY src/main.py .

# Chạy file .py bằng Spark
CMD ["spark-submit", "--jars", "/opt/spark/jars/spark-sql-kafka.jar,/opt/spark/jars/kafka-clients.jar,/opt/spark/jars/spark-token-provider-kafka.jar,/opt/spark/jars/commons-pool2.jar", "main.py"]
