import os
import time
import json
import requests
import urllib3
urllib3.disable_warnings()

LIVY_URL = "https://cdp-se-de-dh-master0.cdp-se-g.rg4u8p.g0.cldrgov.us:443/cdp-se-de-dh/cdp-proxy-token/livy_for_spark3/"
BEARER_TOKEN = os.environ["LIVY_KNOX_BEARER_TOKEN"]
PASSCODE_TOKEN = os.environ["LIVY_KNOX_PASSCODE_TOKEN"]

HEADERS = {
    "Authorization": f"Bearer {BEARER_TOKEN}",
    "X-Passcode": PASSCODE_TOKEN,
    "Content-Type": "application/json"
}

def wait_for_idle(session_id):
    while True:
        r = requests.get(f"{LIVY_URL}sessions/{session_id}", headers=HEADERS, verify=False).json()
        state = r.get("state", "")
        if state == "idle":
            break
        elif state in {"dead", "error", "killed"}:
            raise RuntimeError(f"Session {session_id} failed with state: {state}")
        time.sleep(3)

def wait_for_result(session_id, statement_id):
    while True:
        r = requests.get(f"{LIVY_URL}sessions/{session_id}/statements/{statement_id}", headers=HEADERS, verify=False).json()
        state = r.get("state", "")
        if state == "available":
            return r.get("output", {})
        elif state in {"error", "cancelling"}:
            raise RuntimeError(f"Statement failed: {r}")
        time.sleep(2)

print("[INFO] Creating Livy session...")
resp = requests.post(f"{LIVY_URL}sessions", headers=HEADERS, json={"kind": "pyspark"}, verify=False)
resp.raise_for_status()
session_id = resp.json()["id"]
print(f"[INFO] Livy session ID: {session_id}")
wait_for_idle(session_id)

print("[INFO] Submitting Spark job to write to Iceberg...")
code = """
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("LoadE911JSONToIceberg") \
    .config("spark.sql.catalog.hive_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.hive_catalog.type", "hive") \
    .config("spark.sql.catalog.hive_catalog.uri", "thrift://cdp-se-de-dh-master0.cdp-se-g.rg4u8p.g0.cldrgov.us:9083") \
    .config("spark.sql.catalog.hive_catalog.warehouse", "hdfs:///warehouse/tables") \
    .getOrCreate()

df = spark.read.option("multiline", "true").json("hdfs:///tmp/e911")

# Writes to Iceberg table in Hive catalog (can be seen in JDBC/Impala/Hue)
df.writeTo("hive_catalog.default.e911_calls").using("iceberg").createOrReplace()

"""

submit = requests.post(
    f"{LIVY_URL}sessions/{session_id}/statements",
    headers=HEADERS,
    json={"code": code},
    verify=False
)
submit.raise_for_status()
statement_id = submit.json()["id"]

output = wait_for_result(session_id, statement_id)
print("[INFO] Livy Output:")
print(json.dumps(output, indent=2))

print("[INFO] Deleting session...")
requests.delete(f"{LIVY_URL}sessions/{session_id}", headers=HEADERS, verify=False)
