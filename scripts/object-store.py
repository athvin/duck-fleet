"""Initialize or verify the local S3 fixture. No AWS credentials are consulted."""
import sys
import time

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

client = boto3.client(
    "s3", endpoint_url="http://object-store:9000", region_name="us-east-1",
    aws_access_key_id="DUCKFLEETLOCAL", aws_secret_access_key="local-object-store-password",
    config=Config(connect_timeout=2, read_timeout=5, retries={"max_attempts": 0}),
)
for attempt in range(60):
    try:
        client.list_buckets()
        break
    except (BotoCoreError, ClientError):
        if attempt == 59:
            raise
        time.sleep(1)

if "--verify" in sys.argv:
    pages = client.get_paginator("list_objects_v2").paginate(Bucket="duckfleet", Prefix="lake/")
    files = [item for page in pages for item in page.get("Contents", []) if item["Key"].endswith(".parquet")]
    if not files:
        raise AssertionError("no Parquet objects in DuckLake data prefix")
    # Verify actual Parquet magic, not just filenames.
    for item in files[:3]:
        body = client.get_object(Bucket="duckfleet", Key=item["Key"], Range="bytes=0-3")["Body"].read()
        if body != b"PAR1":
            raise AssertionError("object is not Parquet: " + item["Key"])
    print(f"PASS: {len(files)} Parquet objects in S3; sampled Parquet headers verified")
else:
    try:
        client.create_bucket(Bucket="duckfleet")
    except ClientError as error:
        if error.response["Error"]["Code"] != "BucketAlreadyOwnedByYou":
            raise
    print("Local bucket ready")
