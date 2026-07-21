ARG FLINK_VERSION=2.0.2
FROM apache/flink:${FLINK_VERSION}-scala_2.12

ARG FLINK_VERSION=2.0.2
ARG ICEBERG_VERSION=1.10.1
ARG ICEBERG_FLINK_MAJOR=2.0
ARG COMMONS_CONF2_VERSION=2.1.1
ARG HADOOP_VERSION=3.1.1.7.2.9.0-173-9.0

ARG FLINK_SQL_KAFKA_VERSION=4.0.1-2.0
ARG FLINK_AVRO_REGISTRY_VERSION=2.0.0

RUN set -ex && \
    curl -fsSL -o /opt/flink/lib/iceberg-flink-runtime-${ICEBERG_FLINK_MAJOR}-${ICEBERG_VERSION}.jar \
      "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-${ICEBERG_FLINK_MAJOR}/${ICEBERG_VERSION}/iceberg-flink-runtime-${ICEBERG_FLINK_MAJOR}-${ICEBERG_VERSION}.jar" && \
    curl -fsSL -o /opt/flink/lib/iceberg-aws-bundle-${ICEBERG_VERSION}.jar \
      "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" && \
    curl -fsSL -o /opt/flink/lib/flink-sql-connector-kafka-${FLINK_SQL_KAFKA_VERSION}.jar \
      "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${FLINK_SQL_KAFKA_VERSION}/flink-sql-connector-kafka-${FLINK_SQL_KAFKA_VERSION}.jar" && \
    curl -fsSL -o /opt/flink/lib/flink-sql-avro-confluent-registry-2.0.0.jar \
      "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-avro-confluent-registry/${FLINK_AVRO_REGISTRY_VERSION}/flink-sql-avro-confluent-registry-${FLINK_AVRO_REGISTRY_VERSION}.jar" && \
    curl -fsSL -o /opt/flink/lib/flink-shaded-hadoop-3-uber-${HADOOP_VERSION}.jar \
      "https://repository.cloudera.com/artifactory/cloudera-repos/org/apache/flink/flink-shaded-hadoop-3-uber/${HADOOP_VERSION}/flink-shaded-hadoop-3-uber-${HADOOP_VERSION}.jar" && \
    curl -fsSL -o /opt/flink/lib/commons-configuration2-${COMMONS_CONF2_VERSION}.jar \
      "https://repo1.maven.org/maven2/org/apache/commons/commons-configuration2/${COMMONS_CONF2_VERSION}/commons-configuration2-${COMMONS_CONF2_VERSION}.jar" && \
    curl -fsSL -o /opt/flink/lib/commons-cli-1.5.0.jar \
      "https://repo1.maven.org/maven2/commons-cli/commons-cli/1.5.0/commons-cli-1.5.0.jar"

RUN set -ex && \
    mkdir -p /opt/flink/plugins/s3-fs-hadoop && \
    cp /opt/flink/opt/flink-s3-fs-hadoop-*.jar /opt/flink/plugins/s3-fs-hadoop/ && \
    echo '=== Flink plugins ===' && ls -R /opt/flink/plugins && \
    echo '=== Flink lib ===' && ls /opt/flink/lib

# PyFlink + confluent-kafka for the DLQ Bronze job.
# Base image is a JRE on arm64 → pemja (PyFlink's JNI bridge) has no wheel and
# builds from source, needing gcc + Python.h + jni.h. Install a JDK + build tools
# and point JAVA_HOME at the JDK (its include/ has jni.h) for the pip build.
USER root
RUN set -ex && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
      python3 python3-pip python3-dev build-essential openjdk-17-jdk-headless && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    JAVA_HOME="$(dirname "$(dirname "$(readlink -f "$(which javac)")")")" \
      pip3 install --no-cache-dir \
      apache-flink==2.0.0 "confluent-kafka[avro]==2.5.0" && \
    apt-get clean && rm -rf /var/lib/apt/lists/*