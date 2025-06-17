FROM base-spark:latest

WORKDIR /app
COPY 22120210.py .

# Chạy file .py bằng Spark
CMD ["spark-submit", "--jars", "/opt/spark/jars/spark-sql-kafka.jar,/opt/spark/jars/kafka-clients.jar,/opt/spark/jars/spark-token-provider-kafka.jar,/opt/spark/jars/commons-pool2.jar", "22120210.py"]
