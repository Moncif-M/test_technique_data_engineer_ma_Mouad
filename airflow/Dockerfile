# syntax=docker/dockerfile:1

# Builder stage: download the Hadoop/Delta/Postgres jars once, without keeping
# curl and the build cache in the runtime image.
FROM apache/airflow:2.11.2-python3.11 AS jars

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/*

ARG JARS=/opt/jars
ARG MAVEN=https://repo1.maven.org/maven2
RUN mkdir -p ${JARS} \
 && curl -fsSL -o ${JARS}/hadoop-aws-3.3.4.jar \
      ${MAVEN}/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
 && curl -fsSL -o ${JARS}/aws-java-sdk-bundle-1.12.262.jar \
      ${MAVEN}/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
 && curl -fsSL -o ${JARS}/delta-spark_2.12-3.2.1.jar \
      ${MAVEN}/io/delta/delta-spark_2.12/3.2.1/delta-spark_2.12-3.2.1.jar \
 && curl -fsSL -o ${JARS}/delta-storage-3.2.1.jar \
      ${MAVEN}/io/delta/delta-storage/3.2.1/delta-storage-3.2.1.jar \
 && curl -fsSL -o ${JARS}/postgresql-42.7.13.jar \
      ${MAVEN}/org/postgresql/postgresql/42.7.13/postgresql-42.7.13.jar


# Runtime stage: Java, Python dependencies and the jars from the builder.
FROM apache/airflow:2.11.2-python3.11 AS runtime

ARG JARS=/home/airflow/.local/lib/python3.11/site-packages/pyspark/jars

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
 && rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt pytest==8.3.4

COPY --from=jars --chown=airflow:root /opt/jars/ ${JARS}/
