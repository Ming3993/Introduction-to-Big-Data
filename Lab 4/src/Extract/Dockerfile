# Stage 1: Builder
FROM python:3.11-slim AS builder

WORKDIR /install
COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install --target=/install/target --no-cache-dir -r requirements.txt

# Stage 2: Runtime
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y \
    ca-certificates \
    curl \
    tini \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

ENV PYTHONPATH="${PYTHONPATH}:/usr/local"

COPY --from=builder /install/target /usr/local
COPY 22120210.py .
COPY .env .

EXPOSE ${APP_PORT:-5000}

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python", "22120210.py"]