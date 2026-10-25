FROM apache/airflow:2.11.2-python3.11

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl \
 && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir pyspark==3.5.3 delta-spark==3.2.1 pytest

ARG JARS=/home/airflow/.local/lib/python3.11/site-packages/pyspark/jars
RUN M=https://repo1.maven.org/maven2 \
 && curl -fsSL -o $JARS/hadoop-aws-3.3.4.jar $M/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
 && curl -fsSL -o $JARS/aws-java-sdk-bundle-1.12.262.jar $M/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
 && curl -fsSL -o $JARS/delta-spark_2.12-3.2.1.jar $M/io/delta/delta-spark_2.12/3.2.1/delta-spark_2.12-3.2.1.jar \
 && curl -fsSL -o $JARS/delta-storage-3.2.1.jar $M/io/delta/delta-storage/3.2.1/delta-storage-3.2.1.jar
