FROM base-spark:latest

# Tạo thư mục workdir rõ ràng hơn (tùy chọn)
WORKDIR /app/moving

ENV KAFKA_BROKER=broker:9092

# Copy file từ thư mục con `src` trong context build
ENV KAFKA_BROKER=broker:9092

COPY src/22120210_moving.py .

# Chạy file .py bằng Spark
CMD ["spark-submit", "--jars", "/opt/spark/jars/spark-sql-kafka.jar,/opt/spark/jars/kafka-clients.jar,/opt/spark/jars/spark-token-provider-kafka.jar,/opt/spark/jars/commons-pool2.jar", "/app/moving/22120210_moving.py"]
