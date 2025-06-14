FROM bitnami/spark:3.5.0

# Cài pip và jupyter
USER root
RUN apt-get update && apt-get install -y curl && apt-get install -y python3-pip && pip3 install notebook findspark ipykernel pyspark

# Tải JARs Kafka
WORKDIR /opt/spark/jars
RUN curl -L -O https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar && \
    curl -L -O https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.0/kafka-clients-3.5.0.jar && \
    curl -L -O https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar && \
    curl -L -O https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

# Đặt workdir mặc định
WORKDIR /app
