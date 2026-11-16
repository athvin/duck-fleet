# The dbt-free path, in an image where dbt genuinely is not installed.
#
# Separate from the dbt image on purpose. Running the API workflow inside an image
# that happens to have dbt-core present would prove the workflow works, not that it
# works *without dbt* — and that distinction is the whole of #11. The workflow asserts
# `import dbt` fails before it does anything else.
FROM python:3.12.12-slim-bookworm
WORKDIR /app
COPY clients/duckfleet-python/requirements.lock /app/requirements.lock
RUN pip install --no-cache-dir -r /app/requirements.lock
COPY clients/duckfleet-python /app/client
RUN pip install --no-cache-dir --no-deps /app/client && pip check
COPY tests/api_workflow.py /app/api_workflow.py
ENV PYTHONUNBUFFERED=1
CMD ["python", "/app/api_workflow.py"]
