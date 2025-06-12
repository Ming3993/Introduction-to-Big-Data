FROM bitnami/spark:3.5.0

WORKDIR /app

COPY 22120210_moving.py ./
COPY jars /app/jars

CMD ["/opt/bitnami/spark/bin/spark-submit", "--jars", "/app/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar,/app/jars/kafka-clients-3.5.0.jar,/app/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar,/app/jars/commons-pool2-2.11.1.jar", "22120210_moving.py"]