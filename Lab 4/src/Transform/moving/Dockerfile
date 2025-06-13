# === Stage 1: Downloader ===
FROM alpine:3.18 AS downloader

RUN apk add --no-cache curl
WORKDIR /jars

RUN curl -L -o spark-sql-kafka.jar https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar && \
    curl -L -o kafka-clients.jar https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.0/kafka-clients-3.5.0.jar && \
    curl -L -o spark-token-provider-kafka.jar https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar && \
    curl -L -o commons-pool.jar https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

# === Stage 2: Runtime Spark Container ===
FROM bitnami/spark:3.5.0

WORKDIR /app
COPY --from=downloader /jars /app/jars
RUN ls /app/jars

COPY src/22120210_moving.py ./

CMD ["spark-submit", "--jars", "jars/spark-sql-kafka.jar,jars/kafka-clients.jar,jars/spark-token-provider-kafka.jar,jars/commons-pool.jar", "22120210_moving.py"]