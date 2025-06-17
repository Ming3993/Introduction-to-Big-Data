FROM base-spark:latest

# Tạo thư mục để chứa notebook
WORKDIR /app/notebooks

# Copy file notebook vào container
COPY src/22120210_zscore.ipynb .

# Install papermill to execute the notebook using command line
RUN pip install --no-cache-dir papermill

ENV SPARK_JARS=/opt/spark/jars/spark-sql-kafka.jar,/opt/spark/jars/kafka-clients.jar,/opt/spark/jars/spark-token-provider-kafka.jar,/opt/spark/jars/commons-pool2.jar

# CMD khởi chạy Jupyter (có thể thay đổi)
CMD ["jupyter", "notebook", "--notebook-dir=/app/notebooks", "--ip=0.0.0.0", "--port=8888", "--allow-root", "--NotebookApp.token=''", "--NotebookApp.password=''"]
