# Fabric notebook | default lakehouse attached
# Marketing API (per store account) + Snowflake orders -> bronze Delta tables

import json
from datetime import datetime, timezone
from pathlib import Path

import requests
from pyspark.sql import functions as F

from sourcehub.http import fetch_since
from sourcehub.runner import raise_if_failed, run_sources
from sourcehub.snowflake_extract import connect_from_secrets, extract_incremental

KEY_VAULT = "https://<your-key-vault>.vault.azure.net/"
STATE = Path("/lakehouse/default/Files/_state/sources.json")
STORES = ["store-bog", "store-lim", "store-scl"]


def secret(name):
    return notebookutils.credentials.getSecret(KEY_VAULT, name)  # noqa: F821 (Fabric built-in)


state = json.loads(STATE.read_text()) if STATE.exists() else {}
default_since = "2026-01-01T00:00:00+00:00"
run_started = datetime.now(timezone.utc)

session = requests.Session()
session.headers["Authorization"] = f"Bearer {secret('marketing-api-token')}"


def api_source(store):
    since = datetime.fromisoformat(state.get(store, default_since))
    return lambda: fetch_since(session, f"https://api.example.com/v1/stores/{store}/campaigns", since)


def snowflake_source():
    conn = connect_from_secrets(
        account=secret("sf-account"), user=secret("sf-user"), password=secret("sf-password"),
        warehouse="LOAD_WH", database="ANDES_RETAIL", schema="SALES",
    )
    try:
        rows = []
        for batch in extract_incremental(
            conn, "ORDERS", "UPDATED_AT", ["ORDER_ID", "STORE_ID", "AMOUNT", "UPDATED_AT"],
            since=state.get("snowflake_orders", default_since),
        ):
            rows.extend(batch)
        return rows
    finally:
        conn.close()


sources = {store: api_source(store) for store in STORES}
sources["snowflake_orders"] = snowflake_source
results, failures = run_sources(sources)

for name, rows in results.items():
    if rows:
        table = "bronze_orders" if name == "snowflake_orders" else "bronze_campaigns"
        (
            spark.createDataFrame([{"payload": json.dumps(r, default=str)} for r in rows])  # noqa: F821
            .withColumn("_source", F.lit(name))
            .withColumn("_ingested_at", F.current_timestamp())
            .write.mode("append").saveAsTable(table)
        )
    state[name] = run_started.isoformat()

STATE.parent.mkdir(parents=True, exist_ok=True)
STATE.write_text(json.dumps(state))

raise_if_failed(failures)  # pipeline run goes red and alerts instead of finishing "green" with gaps
