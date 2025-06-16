FROM base-spark:latest

# Tạo thư mục để chứa notebook
WORKDIR /app/notebooks

# Copy file notebook vào container
COPY src/22120210_zscore.ipynb .

ENV SPARK_JARS=/opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar,/opt/spark/jars/kafka-clients-3.5.0.jar,/opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar,/opt/spark/jars/commons-pool2-2.11.1.jar

# CMD khởi chạy Jupyter (có thể thay đổi)
CMD ["jupyter", "notebook", "--notebook-dir=/app/notebooks", "--ip=0.0.0.0", "--port=8888", "--allow-root", "--NotebookApp.token=''", "--NotebookApp.password=''"]
