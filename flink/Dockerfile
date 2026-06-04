FROM flink:1.18-scala_2.12

# ── System deps ──────────────────────────────────────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
        python3 \
        python3-pip \
        python3-dev \
        build-essential \
    && ln -sf /usr/bin/python3 /usr/bin/python \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# ── PyFlink + connectors ──────────────────────────────────────────────────────
RUN pip3 install --no-cache-dir --timeout=600 --retries=5 \
        apache-flink==1.18.0 \
        apache-flink-libraries==1.18.0 \
        mysql-connector-python==8.3.0

# ── Flink SQL connector JARs (Kafka + MySQL CDC + JDBC) ──────────────────────
ARG FLINK_LIB=/opt/flink/lib

# Kafka connector
ADD https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.2-1.18/flink-sql-connector-kafka-3.0.2-1.18.jar \
    ${FLINK_LIB}/

# MySQL JDBC connector
ADD https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.2-1.18/flink-connector-jdbc-3.1.2-1.18.jar \
    ${FLINK_LIB}/

# MySQL JDBC driver
ADD https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.3.0/mysql-connector-j-8.3.0.jar \
    ${FLINK_LIB}/

RUN chmod 644 ${FLINK_LIB}/flink-sql-connector-kafka-*.jar \
              ${FLINK_LIB}/flink-connector-jdbc-*.jar \
              ${FLINK_LIB}/mysql-connector-j-*.jar

# ── Working directory for PyFlink jobs ───────────────────────────────────────
WORKDIR /opt/flink/jobs

EXPOSE 8081 6123 6124
