FROM base-spark:latest

# Tạo thư mục làm việc
WORKDIR /app/notebooks

# Copy file notebook vào container
COPY 22120210_zscore.ipynb .

# Cài thêm jupyter + nbconvert nếu chưa có trong base image
RUN pip install --no-cache-dir jupyter nbconvert

# ENV nếu Spark cần
ENV SPARK_JARS=/opt/spark/jars/spark-sql-kafka.jar,/opt/spark/jars/kafka-clients.jar,/opt/spark/jars/spark-token-provider-kafka.jar,/opt/spark/jars/commons-pool2.jar

# CMD khởi chạy Jupyter (có thể thay đổi)
CMD ["jupyter", "notebook", "--notebook-dir=/app/notebooks", "--ip=0.0.0.0", "--port=8888", "--allow-root", "--NotebookApp.token=''", "--NotebookApp.password=''"]
