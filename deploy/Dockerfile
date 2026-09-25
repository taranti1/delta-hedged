# Reproducible image for recorder, strategy (paper/live) and watchdog roles.
FROM python:3.11-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
RUN apt-get update && apt-get install -y --no-install-recommends chrony ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY pyproject.toml README.md ./
COPY dh ./dh
COPY config ./config
COPY scripts ./scripts
RUN pip install --no-cache-dir .
# secrets are mounted at runtime: /secrets/kalshi_key_id, /secrets/kalshi_private_key.pem
ENTRYPOINT ["python"]
