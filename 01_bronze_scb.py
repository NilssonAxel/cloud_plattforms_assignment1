# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze: SCB
# MAGIC
# MAGIC Downloads two tables from SCB's PxWebApi v2, lands the raw json-stat2 files in the
# MAGIC bronze volume, then appends them to bronze Delta tables.
# MAGIC
# MAGIC | Table | Content | Grain |
# MAGIC |---|---|---|
# MAGIC | `TAB3277` | Newly registered passenger cars | region × fuel type × month |
# MAGIC | `TAB3276` | Passenger cars in traffic, incl. cars per 1,000 inhabitants | region × ownership category × year |
# MAGIC
# MAGIC Bronze is an append-only archive. Every row carries the code **and** the label of the
# MAGIC dimensions people read (`Region`, `Drivmedel`, `Agarkategori`), as they were when the
# MAGIC file was ingested — if SCB renames a municipality, older rows keep the old name.
# MAGIC `ContentsCode` and `Tid` are stored as codes only: the first has one value per table and
# MAGIC the second has a label identical to its code. All values are text; typing is silver's job.

# COMMAND ----------

dbutils.widgets.text("schema_prefix", "", "Schema prefix (e.g. dev_)")
SCHEMA_PREFIX = dbutils.widgets.get("schema_prefix")

CATALOG = "axenil_assignment1"
BRONZE_SCHEMA = f"{SCHEMA_PREFIX}bronze"
VOLUME_PATH = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/raw"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}.raw")

SCB_TABLES = [
    {"table_id": "TAB3277", "contents_code": "TK1001AA", "target": "scb_new_registrations"},
    {"table_id": "TAB3276", "contents_code": "TK1001AB", "target": "scb_cars_in_traffic"},
]
LABELLED_DIMENSIONS = {"Region", "Drivmedel", "Agarkategori"}

FIRST_YEAR = 2016

API_BASE = "https://statistikdatabasen.scb.se/api/v2/tables"
REQUEST_HEADERS = {"User-Agent": "nackademin-laddstolpar-axel"}
MAXIMUM_ATTEMPTS = 5

# COMMAND ----------

import itertools
import json
import math
import os
import time

import requests
from pyspark.sql import functions
from pyspark.sql.types import StringType, StructField, StructType


def get_with_retry(url, parameters=None, timeout=300):
    """GET that retries what is worth retrying and fails loudly on everything else.

    403/429 is SCB's rate limit, 5xx and network errors are transient. Anything else
    (400, 404) means the request itself is wrong, so retrying would not help.
    """
    last_problem = None
    for attempt in range(1, MAXIMUM_ATTEMPTS + 1):
        try:
            response = requests.get(url, params=parameters, headers=REQUEST_HEADERS, timeout=timeout)
        except (requests.ConnectionError, requests.Timeout) as error:
            last_problem = f"{type(error).__name__}: {error}"
        else:
            if response.status_code in (403, 429) or response.status_code >= 500:
                last_problem = f"HTTP {response.status_code}"
            elif response.status_code >= 400:
                raise RuntimeError(
                    f"SCB API rejected the request with HTTP {response.status_code} "
                    f"(not retried): {response.url}\n{response.text[:500]}"
                )
            else:
                return response

        wait_seconds = 15 * attempt
        print(f"  attempt {attempt}/{MAXIMUM_ATTEMPTS} failed ({last_problem}), waiting {wait_seconds}s")
        time.sleep(wait_seconds)

    raise RuntimeError(
        f"SCB API failed {MAXIMUM_ATTEMPTS} times in a row, giving up. "
        f"Last problem: {last_problem}. URL: {url}"
    )


def category_codes(payload, dimension_id):
    """Codes for one dimension, in position order. json-stat allows a list or a dict."""
    index_map = payload["dimension"][dimension_id]["category"]["index"]
    if isinstance(index_map, dict):
        return [code for code, _ in sorted(index_map.items(), key=lambda item: item[1])]
    return list(index_map)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Download
# MAGIC
# MAGIC **json-stat2, not CSV.** The CSV output pivots time into columns, so each year's file
# MAGIC would have different column names. json-stat2 returns a flat value array with a fixed
# MAGIC dimension order, which flattens to one uniform long table.
# MAGIC
# MAGIC **One call and one file per published period** — `TAB3277_2026M09.json` for a month,
# MAGIC `TAB3276_2025.json` for a year. The metadata call lists every period SCB has published;
# MAGIC any period from `FIRST_YEAR` onwards without a file is downloaded. So:
# MAGIC
# MAGIC - a re-run with nothing new published costs one metadata call per table,
# MAGIC - a newly published month is picked up without anyone editing a year constant,
# MAGIC - a failed or deleted period is fetched again on the next run.
# MAGIC
# MAGIC Fewer, larger calls would work too, but per-period files keep the skip check to a
# MAGIC single "does this file exist", and a file name never claims more than it contains.
# MAGIC The first run is ~130 calls; the pause between calls keeps it under SCB's rate limit,
# MAGIC and the retry handles it if not.
# MAGIC
# MAGIC **All regions are downloaded** — municipalities, counties and the national total.
# MAGIC Bronze stores what the source publishes; choosing the municipality grain is silver's job.
# MAGIC
# MAGIC **Writes are atomic.** Each file is written to `.tmp` and renamed on success, so an
# MAGIC interrupted run can't leave a truncated file that the skip check would later treat as
# MAGIC complete.

# COMMAND ----------

def download_table(table_id, contents_code, directory):
    os.makedirs(directory, exist_ok=True)

    metadata = get_with_retry(f"{API_BASE}/{table_id}/metadata", {"lang": "sv"}, timeout=60).json()
    dimension_ids = metadata["id"]
    periods = [code for code in category_codes(metadata, "Tid") if int(code[:4]) >= FIRST_YEAR]

    downloaded = 0
    for period in periods:
        file_path = f"{directory}/{table_id}_{period}.json"
        if os.path.exists(file_path):
            continue

        parameters = {"lang": "sv", "outputFormat": "json-stat2"}
        for dimension_id in dimension_ids:
            if dimension_id == "Tid":
                parameters["valueCodes[Tid]"] = period
            elif dimension_id == "ContentsCode":
                parameters["valueCodes[ContentsCode]"] = contents_code
            else:
                parameters[f"valueCodes[{dimension_id}]"] = "*"

        response = get_with_retry(f"{API_BASE}/{table_id}/data", parameters)

        temporary_path = file_path + ".tmp"
        with open(temporary_path, "wb") as output_file:
            output_file.write(response.content)
        os.replace(temporary_path, file_path)
        downloaded += 1
        time.sleep(0.5)

    print(f"{table_id}: {len(periods)} periods published since {FIRST_YEAR}, {downloaded} downloaded")


for table in SCB_TABLES:
    download_table(table["table_id"], table["contents_code"], f"{VOLUME_PATH}/{table['table_id']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze tables
# MAGIC
# MAGIC Only files not already in the target table are appended (`_source_file` is the
# MAGIC incremental key), which is what makes a re-run add nothing.
# MAGIC
# MAGIC Each period is downloaded once, so a period normally appears once in bronze. If SCB
# MAGIC corrects a period we already have, the runbook fix is to delete that period's file and
# MAGIC re-run: it is downloaded again, appended, and silver keeps the newest version.
# MAGIC
# MAGIC Every column is text with an explicit schema. Bronze should not fail because a source
# MAGIC value changes shape, and region codes keep their leading zeros (`0114` must not become
# MAGIC `114`, since the first two characters are the county code).
# MAGIC
# MAGIC **Row count is verified against the source.** Each json-stat payload declares its own
# MAGIC dimension sizes, and their product is how many cells the file contains. The loader
# MAGIC asserts that the flattened row count matches.

# COMMAND ----------

def flatten_json_stat(payload):
    """Turn a json-stat2 document into (column_names, rows).

    json-stat keeps every value in one flat list with the last dimension varying fastest —
    the same order itertools.product produces, so values can be read off by position.
    """
    dimension_ids = payload["id"]

    column_names = []
    dimension_members = []
    for dimension_id in dimension_ids:
        labels = payload["dimension"][dimension_id]["category"]["label"]
        codes = category_codes(payload, dimension_id)
        if dimension_id in LABELLED_DIMENSIONS:
            column_names += [dimension_id, f"{dimension_id}_label"]
            dimension_members.append([(code, labels[code]) for code in codes])
        else:
            column_names.append(dimension_id)
            dimension_members.append([(code,) for code in codes])
    column_names.append("value")

    values = payload["value"]

    def value_at(position):
        value = values.get(str(position)) if isinstance(values, dict) else values[position]
        return None if value is None else str(value)

    rows = [
        tuple(item for member in combination for item in member) + (value_at(position),)
        for position, combination in enumerate(itertools.product(*dimension_members))
    ]
    return column_names, rows


def load_into_bronze(source_directory, table_name, source_table_id):
    target = f"{CATALOG}.{BRONZE_SCHEMA}.{table_name}"

    if spark.catalog.tableExists(target):
        already_loaded = {
            row["_source_file"]
            for row in spark.table(target).select("_source_file").distinct().collect()
        }
    else:
        already_loaded = set()

    new_rows = []
    column_names = None
    expected_row_count = 0
    new_files = []

    for file_name in sorted(os.listdir(source_directory)):
        if not file_name.endswith(".json") or file_name in already_loaded:
            continue

        with open(f"{source_directory}/{file_name}", "r", encoding="utf-8") as input_file:
            payload = json.load(input_file)

        expected_row_count += math.prod(payload["size"])
        column_names, rows = flatten_json_stat(payload)
        new_rows.extend(row + (source_table_id, file_name) for row in rows)
        new_files.append(file_name)

    if not new_rows:
        print(f"{target}: nothing new to load")
        return

    assert len(new_rows) == expected_row_count, (
        f"{table_name}: flattened {len(new_rows)} rows, "
        f"expected {expected_row_count} from dimension sizes"
    )

    schema = StructType([
        StructField(name, StringType(), True)
        for name in column_names + ["_source_table", "_source_file"]
    ])
    (
        spark.createDataFrame(new_rows, schema)
        .withColumn("_ingested_at", functions.current_timestamp())
        .write.mode("append")
        .saveAsTable(target)
    )
    print(f"{target}: appended {len(new_rows)} rows from {len(new_files)} files: {new_files}")


for table in SCB_TABLES:
    load_into_bronze(f"{VOLUME_PATH}/{table['table_id']}", table["target"], table["table_id"])

# COMMAND ----------

for table in SCB_TABLES:
    target = f"{CATALOG}.{BRONZE_SCHEMA}.{table['target']}"
    print(target, spark.table(target).count(), "rows")

display(spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.scb_new_registrations").limit(20))
