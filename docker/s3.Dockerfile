FROM python:3.12.12-slim-bookworm
RUN pip install --no-cache-dir boto3==1.43.127
COPY scripts/object-store.py /app/object-store.py
ENTRYPOINT ["python", "/app/object-store.py"]
