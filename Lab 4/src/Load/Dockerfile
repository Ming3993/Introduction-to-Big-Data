FROM base-spark:latest

USER root
RUN apt-get update && apt-get install -y python3 python3-pip && \
    pip3 install pymongo kafka-python

WORKDIR /app
COPY 22120210.py .

ENTRYPOINT ["spark-submit", "--jars", "/opt/spark/jars/spark-sql-kafka.jar,/opt/spark/jars/kafka-clients.jar,/opt/spark/jars/spark-token-provider-kafka.jar,/opt/spark/jars/commons-pool2.jar", "22120210.py"]
