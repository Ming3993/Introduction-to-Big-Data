FROM base-spark:latest

# Tạo thư mục workdir rõ ràng hơn (tùy chọn)
WORKDIR /app/moving

# Copy file từ thư mục con `src` trong context build
COPY src/22120210_moving.py .

# Chạy file .py bằng Spark
CMD ["spark-submit", "--jars", "/opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar,/opt/spark/jars/kafka-clients-3.5.0.jar,/opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar,/opt/spark/jars/commons-pool2-2.11.1.jar", "/app/moving/22120210_moving.py"]
