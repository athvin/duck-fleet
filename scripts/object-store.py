"""Initialize or verify the local S3 fixture. No AWS credentials are consulted."""
import json
import os
import subprocess
import sys
import time

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

client = boto3.client(
    "s3", endpoint_url="http://object-store:9000", region_name="us-east-1",
    aws_access_key_id=os.environ.get("DUCKFLEET_OBJECT_ACCESS_KEY", "DUCKFLEETLOCAL"),
    aws_secret_access_key=os.environ.get("DUCKFLEET_OBJECT_SECRET_KEY", "local-object-store-password"),
    config=Config(connect_timeout=2, read_timeout=5, retries={"max_attempts": 0}),
)
reader = boto3.client(
    "s3", endpoint_url="http://object-store:9000", region_name="us-east-1",
    aws_access_key_id="DUCKFLEETREADER",
    aws_secret_access_key=os.environ.get("DUCKFLEET_READER_OBJECT_SECRET_KEY", "local-lake-reader-object-password"),
    config=Config(connect_timeout=2, read_timeout=5, retries={"max_attempts": 0}),
)


def rc(*args, exists_ok=False):
    result = subprocess.run(
        ["rc", *args], capture_output=True, text=True, check=False,
    )
    if result.returncode and not (exists_ok and "exist" in result.stderr.lower()):
        raise RuntimeError(result.stderr.strip() or result.stdout.strip() or "rc failed")


def provision_reader():
    rc("alias", "set", "local", "http://object-store:9000",
       os.environ.get("DUCKFLEET_OBJECT_ACCESS_KEY", "DUCKFLEETLOCAL"),
       os.environ.get("DUCKFLEET_OBJECT_SECRET_KEY", "local-object-store-password"))
    rc("admin", "user", "add", "local", "DUCKFLEETREADER",
       os.environ.get("DUCKFLEET_READER_OBJECT_SECRET_KEY", "local-lake-reader-object-password"), exists_ok=True)
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {"Effect": "Allow", "Action": ["s3:GetObject"], "Resource": "arn:aws:s3:::duckfleet/lake/*"},
            {"Effect": "Allow", "Action": ["s3:ListBucket"], "Resource": "arn:aws:s3:::duckfleet",
             "Condition": {"StringLike": {"s3:prefix": ["lake", "lake/*"]}}},
        ],
    }
    result = subprocess.run(
        ["rc", "admin", "policy", "create", "local", "duckfleet-lake-reader", "/dev/stdin"],
        input=json.dumps(policy), capture_output=True, text=True, check=False,
    )
    if result.returncode and "exist" not in result.stderr.lower():
        raise RuntimeError(result.stderr.strip() or result.stdout.strip() or "reader policy failed")
    rc("admin", "policy", "attach", "local", "duckfleet-lake-reader", "--user", "DUCKFLEETREADER")


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
        body = reader.get_object(Bucket="duckfleet", Key=item["Key"], Range="bytes=0-3")["Body"].read()
        if body != b"PAR1":
            raise AssertionError("object is not Parquet: " + item["Key"])
    for operation in (
        lambda: reader.put_object(Bucket="duckfleet", Key="lake/reader-write-probe", Body=b"denied"),
        lambda: reader.delete_object(Bucket="duckfleet", Key="lake/reader-write-probe"),
        lambda: reader.create_bucket(Bucket="duckfleet-reader-escalation"),
    ):
        try:
            operation()
            raise AssertionError("reader object-store write was not denied")
        except ClientError as error:
            if error.response["Error"]["Code"] not in {"AccessDenied", "AccessDeniedException"}:
                raise
    print(f"PASS: {len(files)} Parquet objects in S3; reader reads and write denials verified")
else:
    try:
        client.create_bucket(Bucket="duckfleet")
    except ClientError as error:
        if error.response["Error"]["Code"] != "BucketAlreadyOwnedByYou":
            raise
    provision_reader()
    print("Local bucket and scoped reader ready")
