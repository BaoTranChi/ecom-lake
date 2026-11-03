# Image Spark dùng chung cho master, worker, history server và job streaming.
# Spark 3.5.3 đi kèm Hadoop 3.3.4 -> hadoop-aws 3.3.4 + aws-java-sdk-bundle 1.12.262 (đúng cặp phiên bản).
FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu

USER root

ARG MVN=https://repo1.maven.org/maven2
# S3A: Spark đọc/ghi MinIO
ADD ${MVN}/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar /opt/spark/jars/
ADD ${MVN}/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar /opt/spark/jars/
# Kafka source/sink cho Structured Streaming (Spark 3.5.x dùng kafka-clients 3.4.1)
ADD ${MVN}/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar /opt/spark/jars/
ADD ${MVN}/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar /opt/spark/jars/
ADD ${MVN}/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar /opt/spark/jars/
ADD ${MVN}/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar /opt/spark/jars/
# GraphFrames (Chương 8) — jar chứa cả mã Scala và gói Python 'graphframes'
ADD https://repos.spark-packages.org/graphframes/graphframes/0.8.3-spark3.5-s_2.12/graphframes-0.8.3-spark3.5-s_2.12.jar /opt/spark/jars/
RUN chmod 644 /opt/spark/jars/*.jar

# numpy: bắt buộc cho pyspark.ml; boto3: quản trị bucket MinIO; pytest: kiểm thử
RUN pip3 install --no-cache-dir numpy==1.26.4 boto3==1.35.36 pytest==8.3.4

# Giải nén gói Python graphframes từ jar vào site-packages để 'import graphframes' chạy được
COPY docker/extract_graphframes.py /tmp/extract_graphframes.py
RUN python3 /tmp/extract_graphframes.py /opt/spark/jars/graphframes-0.8.3-spark3.5-s_2.12.jar

RUN mkdir -p /opt/spark-events /opt/output /data/raw
WORKDIR /opt
