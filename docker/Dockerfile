FROM python:3.11-slim

WORKDIR /app

# System deps: curl for healthchecks, Java for PySpark
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-21-jre-headless \
        curl \
    && rm -rf /var/lib/apt/lists/*

RUN export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java)))) && \
    echo "export JAVA_HOME=$JAVA_HOME" >> /etc/profile.d/java.sh

# Preload Spark / Iceberg / Kafka jars so SparkSession does not have to resolve
# them via Ivy at runtime. Ivy resolution inside the app container was failing
# silently, leaving the `rest` Iceberg catalog unregistered and breaking any
# SQL-path operation (MERGE INTO, spark.table, etc.) on rest.silver.*.
ENV SPARK_JARS_DIR=/opt/spark-jars
RUN set -eux; \
    mkdir -p "$SPARK_JARS_DIR"; \
    base=https://repo1.maven.org/maven2; \
    for coord in \
        "org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.0/iceberg-spark-runtime-3.5_2.12-1.5.0.jar" \
        "org/apache/iceberg/iceberg-aws-bundle/1.5.0/iceberg-aws-bundle-1.5.0.jar" \
        "org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar" \
        "com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar" \
        "org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar" \
        "org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar" \
        "org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar" \
        "org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar" \
    ; do \
        curl -fL --retry 3 --retry-delay 2 -o "$SPARK_JARS_DIR/$(basename "$coord")" "$base/$coord"; \
    done

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Install Python dependencies via uv
COPY pyproject.toml uv.lock* ./
RUN uv sync --frozen --no-dev --extra core

# Copy application source
COPY src/ ./src/
COPY data/ ./data/
COPY scripts/ ./scripts/
COPY tests/ ./tests/
COPY sql/ ./sql/

COPY main.py ./

# Expose Prometheus metrics + FastAPI bridge port
EXPOSE 8000

ENTRYPOINT ["uv", "run", "python", "main.py"]
