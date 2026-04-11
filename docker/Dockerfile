FROM python:3.11-slim

WORKDIR /app

# System deps: curl for healthchecks, Java for PySpark
RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-21-jre-headless \
        curl \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Install Python dependencies via uv
COPY pyproject.toml uv.lock* ./
RUN uv sync --frozen --no-dev --extra core

# Copy application source
COPY src/ ./src/
COPY data/ ./data/
COPY tests/ ./tests/
COPY sql/ ./sql/

COPY main.py ./

# Expose Prometheus metrics + FastAPI bridge port
EXPOSE 8000

ENTRYPOINT ["uv", "run", "python", "main.py"]
