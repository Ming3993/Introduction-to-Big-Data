FROM base-spark:latest

WORKDIR /app/load

ADD https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.13/10.5.0/mongo-spark-connector_2.13-10.5.0.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-sync/4.10.2/mongodb-driver-sync-4.10.2.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/mongodb/bson/4.10.2/bson-4.10.2.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-core/4.10.2/mongodb-driver-core-4.10.2.jar /opt/spark/jars/

COPY 22120210.py .

RUN pip install pymongo

CMD ["spark-submit","--jars", "/opt/spark/jars/spark-sql-kafka.jar,/opt/spark/jars/kafka-clients.jar,/opt/spark/jars/spark-token-provider-kafka.jar,/opt/spark/jars/commons-pool2.jar,/opt/spark/jars/mongo-spark-connector_2.13-10.5.0.jar,/opt/spark/jars/mongodb-driver-sync-4.10.2.jar,/opt/spark/jars/mongodb-driver-core-4.10.2.jar,/opt/spark/jars/bson-4.10.2.jar","/app/load/22120210.py"]
