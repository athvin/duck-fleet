FROM python:3.12.12-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY adapters/dbt-duckfleet/requirements.lock /app/requirements.lock
RUN pip install --no-cache-dir -r /app/requirements.lock
COPY adapters/dbt-duckfleet /app/adapter
RUN pip install --no-cache-dir --no-deps /app/adapter && pip check
COPY tests /app/tests
ENV DBT_PROFILES_DIR=/app/tests/dbt DBT_SEND_ANONYMOUS_USAGE_STATS=false PYTHONUNBUFFERED=1
WORKDIR /app/tests/dbt
CMD ["python", "/app/tests/harness.py"]
