import boto3
import json
import os
import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from urllib.parse import urljoin


# === CONFIGURATION ===
S3_BUCKET = "e911-fake-calls"
SOLR_BASE_URL = "https://cdp-se-gov-demo-dd-leader0.cdp-se-g.rg4u8p.g0.cldrgov.us/cdp-se-gov-demo-dd/cdp-proxy-token/solr/"
COLLECTION_NAME = "e911_demo_collection"
REGION = "us-gov-west-1"

# Knox Bearer Token
BEARER_TOKEN = os.environ.get("KNOX_BEARER_TOKEN")
if not BEARER_TOKEN:
    raise RuntimeError("KNOX_BEARER_TOKEN environment variable not set.")

HEADERS = {
    "Authorization": f"Bearer {BEARER_TOKEN}",
    "Content-Type": "application/json"
}

# === Initialize S3 ===
s3 = boto3.client(
    "s3",
    region_name=REGION,
    aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY")
)

# === Helper: Check if Solr collection exists ===
def collection_exists():
    url = urljoin(SOLR_BASE_URL, "admin/collections?action=LIST&wt=json")
    resp = requests.get(url, headers=HEADERS, verify=False)
    try:
        collections = resp.json().get("collections", [])
        return COLLECTION_NAME in collections
    except Exception:
        print(f"[ERROR] Failed to list collections. Status: {resp.status_code}, Body: {resp.text}")
        return False

# === Helper: Create Solr collection ===
def create_collection():
    url = urljoin(SOLR_BASE_URL, f"admin/collections?action=CREATE&name={COLLECTION_NAME}&numShards=1&replicationFactor=1&wt=json")
    resp = requests.get(url, headers=HEADERS, verify=False)
    print(f"[INFO] Create collection response: {resp.status_code} {resp.text}")

# === Helper: Enable auto field creation ===
def enable_auto_fields():
    url = urljoin(SOLR_BASE_URL, f"{COLLECTION_NAME}/config")
    payload = {
        "set-user-property": {
            "update.autoCreateFields": "true"
        }
    }
    resp = requests.post(url, headers=HEADERS, data=json.dumps(payload), verify=False)
    print(f"[INFO] Auto-create fields: {resp.status_code} {resp.text}")

# === Helper: Index a document ===
def post_document(doc):
    url = urljoin(SOLR_BASE_URL, f"{COLLECTION_NAME}/update/json/docs?commit=true")
    resp = requests.post(url, headers=HEADERS, data=json.dumps(doc), verify=False)
    if resp.status_code != 200:
        print(f"[ERROR] Failed to index document: {resp.text}")
    else:
        print(f"[✓] Document indexed")

# === Main Flow ===
if not collection_exists():
    print(f"[INFO] Solr collection '{COLLECTION_NAME}' does not exist. Creating it...")
    create_collection()
    enable_auto_fields()
else:
    print(f"[INFO] Solr collection '{COLLECTION_NAME}' already exists.")

# === Load and Index S3 JSON files ===
print(f"[INFO] Listing S3 objects in bucket '{S3_BUCKET}'...")
paginator = s3.get_paginator('list_objects_v2')
pages = paginator.paginate(Bucket=S3_BUCKET)

for page in pages:
    for obj in page.get("Contents", []):
        key = obj["Key"]
        if not key.lower().endswith(".json"):
            continue

        print(f"[INFO] Loading file: {key}")
        s3_obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        content = s3_obj["Body"].read().decode("utf-8")

        try:
            data = json.loads(content)
            post_document(data)
        except json.JSONDecodeError:
            print(f"[ERROR] Invalid JSON in file: {key}")