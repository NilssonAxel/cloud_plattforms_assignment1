# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Bronze: electricity prices
# MAGIC
# MAGIC Downloads daily spot prices per price area from elprisetjustnu.se and lands
# MAGIC one JSON file per (price area, date) in the bronze volume.
# MAGIC
# MAGIC The API has no query parameters — date and area are path segments, so one call
# MAGIC returns one day for one area. History starts around November 2022.
# MAGIC
# MAGIC | Area | Region |
# MAGIC |---|---|
# MAGIC | SE1 | Luleå |
# MAGIC | SE2 | Sundsvall |
# MAGIC | SE3 | Stockholm |
# MAGIC | SE4 | Malmö |
# MAGIC
# MAGIC **This is the slowest part of the pipeline on a first run:** four calls per day since
# MAGIC November 2022 (about 5,700 calls by autumn 2026, growing by 4 a day), roughly two hours.
# MAGIC Files are skipped if already present, so a re-run only fetches new days. Which files
# MAGIC exist is read with one listing per folder, not one check per file (see
# MAGIC `list_existing_days`).
# MAGIC
# MAGIC Same conventions as `01`: raw files are the archive, written atomically; the bronze table
# MAGIC stores every value as text; `_source_file` is the path relative to this source's raw
# MAGIC folder (`elomrade=SE3/ar=2024/2024-01-15.json`), so moving or renaming the volume never
# MAGIC makes old files look new.
# MAGIC
# MAGIC Data is free to use; elprisetjustnu.se asks for attribution.

# COMMAND ----------

import os
import time
import datetime
import requests

dbutils.widgets.text("schema_prefix", "", "Schema prefix (e.g. dev_)")
SCHEMA_PREFIX = dbutils.widgets.get("schema_prefix")

CATALOG = "axenil_assignment1"
BRONZE_SCHEMA = f"{SCHEMA_PREFIX}bronze"
RAW_DIRECTORY = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/raw/elpris"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}.raw")

PRICE_API_BASE = "https://www.elprisetjustnu.se/api/v1/prices"
PRICE_AREAS = ["SE1", "SE2", "SE3", "SE4"]
REQUEST_HEADERS = {"User-Agent": "nackademin-laddstolpar-axel"}

FIRST_DATE = datetime.date(2022, 11, 1)
LAST_DATE = datetime.date.today()

SECONDS_BETWEEN_REQUESTS = 0.3
MAXIMUM_ATTEMPTS = 5

# COMMAND ----------

def build_price_url(date, price_area):
    return f"{PRICE_API_BASE}/{date.year}/{date.strftime('%m-%d')}_{price_area}.json"


def list_existing_days():
    """Every (area, ISO date) that already has a file, from one listing per folder.

    The volume is cloud storage, so each existence check is a network round trip. Listing
    each area/year folder once (about 16 calls) and checking against this set in memory
    replaces one call per day and area (about 11,000 with the gap check), which made a
    run with nothing new to fetch take ~10 minutes.

    The set is a snapshot taken at the start of the run. That is safe because this notebook
    is the only writer to the folder and the job allows one run at a time.
    """
    existing = set()
    if not os.path.isdir(RAW_DIRECTORY):
        return existing
    for area_directory in os.listdir(RAW_DIRECTORY):
        price_area = area_directory.split("=")[1]
        for year_directory in os.listdir(f"{RAW_DIRECTORY}/{area_directory}"):
            for file_name in os.listdir(f"{RAW_DIRECTORY}/{area_directory}/{year_directory}"):
                if file_name.endswith(".json"):
                    existing.add((price_area, file_name.removesuffix(".json")))
    return existing


existing_days = list_existing_days()
print(f"{len(existing_days)} (area, day) files already present")


def fetch_price_day(date, price_area):
    """Download one (date, area) file. Returns fetched, skipped or missing.

    404 means the API has no data for that day (before the history starts, or tomorrow's
    prices not published yet) and is not an error. 429, 5xx and network errors are retried
    with a growing wait; after MAXIMUM_ATTEMPTS the run stops with an error naming the URL,
    so a gap in the data can never pass silently.
    """
    directory = f"{RAW_DIRECTORY}/elomrade={price_area}/ar={date.year}"
    file_path = f"{directory}/{date.isoformat()}.json"

    if (price_area, date.isoformat()) in existing_days:
        return "skipped"

    url = build_price_url(date, price_area)
    last_problem = None
    for attempt in range(1, MAXIMUM_ATTEMPTS + 1):
        try:
            response = requests.get(url, headers=REQUEST_HEADERS, timeout=60)
        except (requests.ConnectionError, requests.Timeout) as error:
            last_problem = f"{type(error).__name__}: {error}"
        else:
            if response.status_code == 404:
                return "missing"
            if response.status_code == 429 or response.status_code >= 500:
                last_problem = f"HTTP {response.status_code}"
            elif response.status_code >= 400:
                raise RuntimeError(
                    f"elprisetjustnu.se rejected the request with HTTP {response.status_code} "
                    f"(not retried): {url}"
                )
            else:
                os.makedirs(directory, exist_ok=True)
                temporary_path = file_path + ".tmp"
                with open(temporary_path, "wb") as output_file:
                    output_file.write(response.content)
                os.replace(temporary_path, file_path)
                existing_days.add((price_area, date.isoformat()))
                return "fetched"

        wait_seconds = 10 * attempt
        print(f"  {url}: attempt {attempt}/{MAXIMUM_ATTEMPTS} failed ({last_problem}), waiting {wait_seconds}s")
        time.sleep(wait_seconds)

    raise RuntimeError(
        f"elprisetjustnu.se failed {MAXIMUM_ATTEMPTS} times in a row, giving up. "
        f"Last problem: {last_problem}. URL: {url}"
    )


result_counts = {"fetched": 0, "skipped": 0, "missing": 0}
current_date = FIRST_DATE

while current_date <= LAST_DATE:
    for price_area in PRICE_AREAS:
        result = fetch_price_day(current_date, price_area)
        result_counts[result] += 1
        if result == "fetched":
            time.sleep(SECONDS_BETWEEN_REQUESTS)

    if current_date.day == 1:
        print(current_date, result_counts)
    current_date += datetime.timedelta(days=1)

print("done:", result_counts)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gap check
# MAGIC
# MAGIC A 404 is normal for today before the day's prices exist, but a past day that is still
# MAGIC missing means a hole in the data. The run stops so someone looks at it, instead of the
# MAGIC average prices quietly being computed from fewer days. A day the API genuinely never
# MAGIC published goes into `KNOWN_MISSING_DAYS` after checking, so the run passes again.

# COMMAND ----------

KNOWN_MISSING_DAYS = set()
GRACE_DAYS = 2

missing_days = []
current_date = FIRST_DATE
while current_date <= LAST_DATE - datetime.timedelta(days=GRACE_DAYS):
    for price_area in PRICE_AREAS:
        key = (price_area, current_date.isoformat())
        if key not in KNOWN_MISSING_DAYS and key not in existing_days:
            missing_days.append(f"{price_area} {current_date.isoformat()}")
    current_date += datetime.timedelta(days=1)

if missing_days:
    raise RuntimeError(
        f"{len(missing_days)} past (area, day) files are missing although the API was asked for "
        f"them — the API answered 404. First ones: {missing_days[:10]}. Check the URL by hand; if "
        f"the day really does not exist upstream, add it to KNOWN_MISSING_DAYS."
    )
print("gap check: no missing days")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze table

# COMMAND ----------

import json
from pyspark.sql import functions
from pyspark.sql.types import StringType, StructField, StructType

TARGET_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.electricity_prices"

PRICE_COLUMNS = ["time_start", "time_end", "SEK_per_kWh", "EUR_per_kWh", "EXR"]
PRICE_SCHEMA = StructType(
    [StructField("elomrade", StringType(), False)]
    + [StructField(column, StringType(), True) for column in PRICE_COLUMNS]
    + [StructField("_source_file", StringType(), False)]
)


def as_text(value):
    return None if value is None else str(value)


if spark.catalog.tableExists(TARGET_TABLE):
    already_loaded = {
        row["_source_file"]
        for row in spark.table(TARGET_TABLE).select("_source_file").distinct().collect()
    }
else:
    already_loaded = set()

new_rows = []

for area_directory in sorted(os.listdir(RAW_DIRECTORY)):
    price_area = area_directory.split("=")[1]

    for year_directory in sorted(os.listdir(f"{RAW_DIRECTORY}/{area_directory}")):
        year_path = f"{RAW_DIRECTORY}/{area_directory}/{year_directory}"

        for file_name in sorted(os.listdir(year_path)):
            if not file_name.endswith(".json"):
                continue

            source_file = f"{area_directory}/{year_directory}/{file_name}"
            if source_file in already_loaded:
                continue

            with open(f"{year_path}/{file_name}", "r", encoding="utf-8") as input_file:
                periods = json.load(input_file)

            for period in periods:
                new_rows.append(
                    (price_area,)
                    + tuple(as_text(period.get(column)) for column in PRICE_COLUMNS)
                    + (source_file,)
                )

new_file_count = len({row[-1] for row in new_rows})
print(f"{len(new_rows)} new rows from {new_file_count} files")

if new_rows:
    (
        spark.createDataFrame(new_rows, PRICE_SCHEMA)
        .withColumn("_ingested_at", functions.current_timestamp())
        .write.mode("append")
        .saveAsTable(TARGET_TABLE)
    )

print("total rows:", spark.table(TARGET_TABLE).count())

# COMMAND ----------

electricity_prices = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.electricity_prices")

display(
    electricity_prices.groupBy("elomrade")
          .agg(
              functions.countDistinct("_source_file").alias("amount_days"),
              functions.count("*").alias("amount_hours"),
              functions.min("time_start").alias("earliest"),
              functions.max("time_start").alias("latest"),
          )
          .orderBy("elomrade")
)