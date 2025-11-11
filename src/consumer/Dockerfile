# Dockerfile for Spark consumer
FROM apache/spark:3.5.1

WORKDIR /opt/src/consumer

COPY . /opt/src/consumer
# Install Python dependencies
COPY  requirements.txt /tmp/requirements.txt
USER root
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Create jars directory if it doesn't exist
RUN mkdir -p /opt/spark/jars

# Download Kafka connector + dependencies for Spark 3.5.1
RUN curl -L -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar && \
    curl -L -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar && \
    curl -L -o /opt/spark/jars/kafka-clients-3.5.1.jar \
      https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar && \
    curl -L -o /opt/spark/jars/commons-pool2-2.11.1.jar \
      https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar



USER spark


# Set working directory for your Spark jobs

# # Pre-download the Spark-Kafka connector using --packages
# RUN /opt/spark/bin/spark-submit \
#     --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1 \
#     --version || true

# Set the default command to run your consumer
CMD ["/opt/spark/bin/spark-submit", "/opt/src/consumer/spark_consumer.py"]

