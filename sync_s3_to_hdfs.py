## RUN FROM SPARK3 DH HOST WITH LIVY (DONT FORGET TO KINIT)

import boto3
import os
import json
import subprocess
from pathlib import Path

# === CONFIGURATION ===
S3_BUCKET = "e911-fake-calls"
REGION = "us-gov-west-1"
LOCAL_DIR = "/tmp/e911_s3/"
HDFS_DIR = "/tmp/e911/"

# === AWS Credentials (fill in as needed) ===
AWS_ACCESS_KEY_ID = "X"
AWS_SECRET_ACCESS_KEY = "X"

# === Step 1: Download from S3 ===
print(f"[INFO] Downloading files from S3 bucket '{S3_BUCKET}' to '{LOCAL_DIR}'...")
Path(LOCAL_DIR).mkdir(parents=True, exist_ok=True)

s3 = boto3.client(
    "s3",
    region_name=REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

paginator = s3.get_paginator("list_objects_v2")
pages = paginator.paginate(Bucket=S3_BUCKET)

for page in pages:
    for obj in page.get("Contents", []):
        key = obj["Key"]
        if not key.lower().endswith(".json"):
            continue
        local_path = os.path.join(LOCAL_DIR, os.path.basename(key))
        body = s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read().decode("utf-8")
        try:
            json.loads(body)  # Validate
            with open(local_path, "w") as f:
                f.write(body)
            print(f"[✓] Downloaded {key}")
        except json.JSONDecodeError:
            print(f"[WARN] Invalid JSON skipped: {key}")

# === Step 2: Copy to HDFS ===
import glob

print(f"[INFO] Ensuring HDFS directory {HDFS_DIR} exists...")
subprocess.run(["hdfs", "dfs", "-mkdir", "-p", HDFS_DIR], check=True)

print(f"[INFO] Uploading files to HDFS {HDFS_DIR}...")
json_files = glob.glob(f"{LOCAL_DIR}*.json")
if not json_files:
    raise RuntimeError(f"No JSON files found in {LOCAL_DIR}")

subprocess.run(["hdfs", "dfs", "-copyFromLocal", "-f"] + json_files + [HDFS_DIR], check=True)

print("[✅] S3 to HDFS sync complete.")
