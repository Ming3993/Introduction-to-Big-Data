FROM bitnami/spark:latest

USER root
RUN apt-get update && apt-get install -y python3 python3-pip && \
    pip3 install pymongo kafka-python

COPY load_streaming.py /app/load_streaming.py

ENTRYPOINT ["spark-submit", "/app/load_streaming.py"]
