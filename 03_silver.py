# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Silver
# MAGIC
# MAGIC Typed, filtered, deduplicated and quality-checked. Source vocabulary becomes project
# MAGIC vocabulary: `Region`, `Drivmedel` and `Tid` become `municipality_code`, `fuel_type_code`
# MAGIC and real `DATE` columns.
# MAGIC
# MAGIC What this layer does:
# MAGIC
# MAGIC - Filters Region to 4-character municipality codes. National and county rows are
# MAGIC   aggregates of these and would double-count in a `SUM`.
# MAGIC - Drops region code `1917` (Heby, pre-2007 code, no data in our period).
# MAGIC - Types every column: counts become `INT`, months and years become `DATE`.
# MAGIC - Keeps only the newest version of each business key, in case a period was downloaded
# MAGIC   again (e.g. after SCB corrected it).
# MAGIC - Turns ownership category `060` (cars per 1,000 inhabitants) into an inhabitants
# MAGIC   estimate, and keeps it out of the vehicle counts — a ratio must not sit in a count column.
# MAGIC - Aggregates hourly and 15-minute prices to one row per price area and day.
# MAGIC - Maps municipality to price area through a maintained county-level seed.
# MAGIC
# MAGIC **Order of work: build → check → write.** All seven tables are built as DataFrames first,
# MAGIC then checked (see `quality_checks`). Only if no error-level check fails are they written,
# MAGIC so silver is either fully updated or untouched — a failed run leaves nothing to clean up.
# MAGIC Writes use `MERGE` on the business key, so a re-run updates in place rather than
# MAGIC appending, and only rows whose values changed are rewritten.

# COMMAND ----------

# MAGIC %run ./quality_checks

# COMMAND ----------

dbutils.widgets.text("schema_prefix", "", "Schema prefix (e.g. dev_)")
dbutils.widgets.text("run_id", "manual", "Job run id (set by the job)")
SCHEMA_PREFIX = dbutils.widgets.get("schema_prefix")
RUN_ID = dbutils.widgets.get("run_id")

CATALOG = "axenil_assignment1"
BRONZE_SCHEMA = f"{SCHEMA_PREFIX}bronze"
SILVER_SCHEMA = f"{SCHEMA_PREFIX}silver"
OPS_SCHEMA = f"{SCHEMA_PREFIX}ops"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{OPS_SCHEMA}")

checks = QualityChecks(f"{CATALOG}.{OPS_SCHEMA}.quality_check_results", RUN_ID, "silver")

# COMMAND ----------

from pyspark.sql import Window, functions


def keep_newest(dataframe, key_columns, order_columns):
    """One row per business key: the one that sorts first on order_columns (descending)."""
    newest_first = Window.partitionBy(*key_columns).orderBy(
        *[functions.col(column).desc() for column in order_columns]
    )
    return (
        dataframe
        .withColumn("_row_number", functions.row_number().over(newest_first))
        .filter(functions.col("_row_number") == 1)
        .drop("_row_number")
    )


def deduplicate(dataframe, key_columns):
    """One row per business key, the newest by _ingested_at.

    MERGE protects the target across runs but does not deduplicate the source, so a
    duplicate business key in bronze would otherwise insert twice on a first load.
    """
    if "_ingested_at" not in dataframe.columns:
        return dataframe
    return keep_newest(dataframe, key_columns, ["_ingested_at"]).drop("_ingested_at")


def merge_into_silver(source_dataframe, table_name, key_columns):
    """MERGE an already deduplicated and checked DataFrame into the target on its business key.

    A matched row is only updated when one of its values actually differs (`<=>` is the
    null-safe equality), so a re-run with no new data rewrites nothing.
    """
    target = f"{CATALOG}.{SILVER_SCHEMA}.{table_name}"

    source_dataframe.createOrReplaceTempView("merge_source")

    if not spark.catalog.tableExists(target):
        source_dataframe.limit(0).write.saveAsTable(target)

    key_condition = " AND ".join(f"target.{column} = source.{column}" for column in key_columns)
    value_columns = [column for column in source_dataframe.columns if column not in key_columns]
    changed_condition = " OR ".join(
        f"NOT (target.{column} <=> source.{column})" for column in value_columns
    ) or "FALSE"

    metrics = spark.sql(f"""
        MERGE INTO {target} AS target
        USING merge_source AS source
          ON {key_condition}
        WHEN MATCHED AND ({changed_condition}) THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """).first()
    print(
        f"{table_name}: {metrics['num_inserted_rows']} inserted, "
        f"{metrics['num_updated_rows']} updated, {spark.table(target).count()} rows in total"
    )

# COMMAND ----------

# Mapping from county to electricity price area.
# Source: Svenska kraftnät price area map, read 2026-09-24.
# Rule: each county is assigned the price area covering most of its area.
# Counties split by the SE2/SE3 and SE3/SE4 boundaries are flagged below —
# Kalmar for example has roughly 30 % of its area in SE3 while mapped to SE4.
COUNTY_TO_PRICE_AREA = [
    ("25", "SE1"), ("24", "SE2"), ("23", "SE2"), ("22", "SE2"), ("21", "SE2"),
    ("01", "SE3"), ("03", "SE3"), ("04", "SE3"), ("05", "SE3"), ("06", "SE3"),
    ("09", "SE3"), ("14", "SE3"), ("17", "SE3"), ("18", "SE3"), ("19", "SE3"),
    ("20", "SE3"),
    ("07", "SE4"), ("08", "SE4"), ("10", "SE4"), ("12", "SE4"), ("13", "SE4"),
]
SPLIT_COUNTIES = ["07", "08", "13", "20", "21"]

# 1917 = Heby, pre-2007 code before the county change (now 0331).
DISCONTINUED_REGION_CODES = ["1917"]

# Ownership categories that are counts. 000 = 010 + 020 + 030, and 040 (taxi) is a
# subset of 030, so these must never be summed across categories.
COUNT_OWNERSHIP_CODES = ["000", "010", "020", "030", "040"]
# 050 = private persons' cars per 1,000 inhabitants, 060 = all cars per 1,000 inhabitants.
RATIO_OWNERSHIP_CODES = ["050", "060"]
CARS_PER_1000_INHABITANTS_CODE = "060"
TOTAL_OWNERSHIP_CODE = "000"

# Every fuel code SCB published when this was written. A new code still flows through to
# silver and gold, but someone has to decide whether it belongs in ELECTRIFIED_FUEL_CODES.
KNOWN_FUEL_CODES = ["100", "110", "120", "130", "140", "150", "160", "190"]
ELECTRIFIED_FUEL_CODES = ["120", "140"]

new_registrations_bronze = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.scb_new_registrations")
cars_in_traffic_bronze = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.scb_cars_in_traffic")


def is_municipality(region_column):
    return (functions.length(region_column) == 4) & ~region_column.isin(DISCONTINUED_REGION_CODES)


# Every silver table, built below: name -> (deduplicated DataFrame, business key).
# Nothing is written until the checks further down have passed.
silver_tables = {}


def register(table_name, dataframe, key_columns):
    silver_tables[table_name] = (deduplicate(dataframe, key_columns), key_columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimensions
# MAGIC
# MAGIC Built from the code and label pairs carried in bronze. If a label has changed over time,
# MAGIC the one from the most recent period wins.

# COMMAND ----------

def latest_labels(dataframe, code_column, label_column):
    return keep_newest(
        dataframe.select(
            functions.col(code_column).alias("code"),
            functions.col(label_column).alias("label"),
            "Tid",
            "_ingested_at",
        ),
        ["code"],
        ["Tid", "_ingested_at"],
    ).select("code", "label")


region_labels = latest_labels(new_registrations_bronze, "Region", "Region_label")

municipalities = (
    region_labels
    .filter(is_municipality(functions.col("code")))
    .select(
        functions.col("code").alias("municipality_code"),
        functions.col("label").alias("municipality_name"),
        functions.substring("code", 1, 2).alias("county_code"),
    )
)

counties = (
    region_labels
    .filter(functions.length("code") == 2)
    .select(
        functions.col("code").alias("county_code"),
        functions.col("label").alias("county_name"),
    )
)

price_areas = spark.createDataFrame(COUNTY_TO_PRICE_AREA, ["county_code", "price_area"])

municipality = (
    municipalities
    .join(counties, on="county_code", how="left")
    .join(price_areas, on="county_code", how="left")
    .withColumn("price_area_is_split", functions.col("county_code").isin(SPLIT_COUNTIES))
    .select("municipality_code", "municipality_name", "county_code",
            "county_name", "price_area", "price_area_is_split")
)
register("municipality", municipality, ["municipality_code"])

fuel_type = (
    latest_labels(new_registrations_bronze, "Drivmedel", "Drivmedel_label")
    .select(
        functions.col("code").alias("fuel_type_code"),
        functions.col("label").alias("fuel_type_name"),
    )
    .withColumn("is_electrified", functions.col("fuel_type_code").isin(ELECTRIFIED_FUEL_CODES))
)
register("fuel_type", fuel_type, ["fuel_type_code"])

ownership_category = (
    latest_labels(cars_in_traffic_bronze, "Agarkategori", "Agarkategori_label")
    .filter(functions.col("code").isin(COUNT_OWNERSHIP_CODES))
    .select(
        functions.col("code").alias("ownership_category_code"),
        functions.col("label").alias("ownership_category_name"),
    )
)
register("ownership_category", ownership_category, ["ownership_category_code"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Facts
# MAGIC
# MAGIC `Tid` arrives as `2024M01` for months and `2024` for years. Months become the first day
# MAGIC of the month. Cars in traffic is SCB's count at year end, so the year becomes 31
# MAGIC December; `year` is kept as well because that is how people filter it.

# COMMAND ----------

new_registrations = (
    new_registrations_bronze
    .filter(is_municipality(functions.col("Region")))
    .select(
        functions.col("Region").alias("municipality_code"),
        functions.to_date(functions.col("Tid"), "yyyy'M'MM").alias("registration_month"),
        functions.col("Drivmedel").alias("fuel_type_code"),
        functions.col("value").cast("int").alias("registration_count"),
        "_ingested_at",
    )
)
register("new_registrations", new_registrations, ["municipality_code", "registration_month", "fuel_type_code"])

# COMMAND ----------

cars_in_traffic_typed = (
    cars_in_traffic_bronze
    .filter(is_municipality(functions.col("Region")))
    .select(
        functions.col("Region").alias("municipality_code"),
        functions.col("Tid").cast("int").alias("year"),
        functions.col("Agarkategori").alias("ownership_category_code"),
        functions.col("value").cast("int").alias("value"),
        "_ingested_at",
    )
    .withColumn("reference_date", functions.make_date("year", functions.lit(12), functions.lit(31)))
)

cars_in_traffic = (
    cars_in_traffic_typed
    .filter(functions.col("ownership_category_code").isin(COUNT_OWNERSHIP_CODES))
    .select(
        "municipality_code", "year", "reference_date", "ownership_category_code",
        functions.col("value").alias("vehicle_count"),
        "_ingested_at",
    )
)
register("cars_in_traffic", cars_in_traffic, ["municipality_code", "year", "ownership_category_code"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inhabitants
# MAGIC
# MAGIC The assignment's second SCB source is cars **and inhabitants** per municipality. TAB3276
# MAGIC does not publish inhabitants directly, but category `060` is total cars per 1,000
# MAGIC inhabitants, so:
# MAGIC
# MAGIC `inhabitants ≈ cars_total / cars_per_1000_inhabitants × 1000`
# MAGIC
# MAGIC SCB rounds the ratio to a whole number, so the estimate is within about ±0.15 % — e.g.
# MAGIC 412 means 411.5–412.5. Good enough to compare municipalities, and it keeps the pipeline
# MAGIC on the sources the assignment specifies.

# COMMAND ----------

typed_latest = keep_newest(
    cars_in_traffic_typed, ["municipality_code", "year", "ownership_category_code"], ["_ingested_at"]
)

inhabitants = (
    typed_latest
    .groupBy("municipality_code", "year", "reference_date")
    .agg(
        functions.max(functions.when(
            functions.col("ownership_category_code") == TOTAL_OWNERSHIP_CODE, functions.col("value")
        )).alias("cars_total"),
        functions.max(functions.when(
            functions.col("ownership_category_code") == CARS_PER_1000_INHABITANTS_CODE, functions.col("value")
        )).alias("cars_per_1000_inhabitants"),
    )
    .withColumn(
        "inhabitants_estimate",
        functions.round(
            functions.col("cars_total") / functions.col("cars_per_1000_inhabitants") * 1000
        ).cast("int"),
    )
    .select("municipality_code", "year", "reference_date",
            "cars_per_1000_inhabitants", "inhabitants_estimate")
)
register("inhabitants", inhabitants, ["municipality_code", "year"])

# COMMAND ----------

daily_prices = (
    spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.electricity_prices")
    .select(
        functions.col("elomrade").alias("price_area"),
        # local date as published — avoids timezone conversion entirely
        functions.to_date(functions.substring("time_start", 1, 10)).alias("price_date"),
        "time_start",
        functions.col("SEK_per_kWh").cast("double").alias("sek_per_kwh"),
    )
    # One price per area and period, even if the same day was ever loaded twice.
    .dropDuplicates(["price_area", "time_start"])
    .groupBy("price_area", "price_date")
    .agg(
        functions.avg("sek_per_kwh").alias("avg_sek_per_kwh"),
        functions.min("sek_per_kwh").alias("min_sek_per_kwh"),
        functions.max("sek_per_kwh").alias("max_sek_per_kwh"),
        functions.count("*").cast("int").alias("period_count"),
    )
    .withColumn(
        "resolution_minutes",
        functions.when(functions.col("period_count") > 30, 15).otherwise(60),
    )
)

register("electricity_prices_daily", daily_prices, ["price_area", "price_date"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality checks — before writing
# MAGIC
# MAGIC Run on the built DataFrames. Each is a set of offending rows; see `quality_checks` for
# MAGIC how they are logged and enforced.

# COMMAND ----------

municipality = silver_tables["municipality"][0]
fuel_type = silver_tables["fuel_type"][0]
new_registrations = silver_tables["new_registrations"][0]
cars_in_traffic = silver_tables["cars_in_traffic"][0]
inhabitants = silver_tables["inhabitants"][0]
daily_prices = silver_tables["electricity_prices_daily"][0]

EXPECTED_MUNICIPALITY_COUNT = 290  # Sweden since 2003. A merger must be a deliberate change here.

municipality_count = municipality.count()
checks.condition(
    "municipality", f"exactly {EXPECTED_MUNICIPALITY_COUNT} municipalities",
    municipality_count == EXPECTED_MUNICIPALITY_COUNT, f"found {municipality_count}",
)
checks.rows("municipality", "every municipality has a county name and a price area",
            municipality.filter(functions.col("county_name").isNull() | functions.col("price_area").isNull()))

checks.rows("fuel_type", "every fuel type has a name",
            fuel_type.filter(functions.col("fuel_type_name").isNull()))

checks.rows("new_registrations", "every period parsed to a date",
            new_registrations.filter(functions.col("registration_month").isNull()))
checks.rows("new_registrations", "no missing or negative counts",
            new_registrations.filter(functions.col("registration_count").isNull()
                                     | (functions.col("registration_count") < 0)))
checks.rows("new_registrations", "every municipality exists in the dimension",
            new_registrations.join(municipality, "municipality_code", "left_anti")
            .select("municipality_code").distinct())
checks.rows("new_registrations", "every fuel type exists in the dimension",
            new_registrations.join(fuel_type, "fuel_type_code", "left_anti")
            .select("fuel_type_code").distinct())

grid = new_registrations.agg(
    functions.count("*").alias("rows"),
    functions.countDistinct("municipality_code").alias("municipalities"),
    functions.countDistinct("fuel_type_code").alias("fuel_types"),
    functions.countDistinct("registration_month").alias("months"),
).first()
expected_rows = grid["municipalities"] * grid["fuel_types"] * grid["months"]
checks.condition(
    "new_registrations", "complete grid: every municipality has every fuel type every month",
    grid["rows"] == expected_rows, f"{grid['rows']} rows, expected {expected_rows} ({grid.asDict()})",
)

cars_by_category = (
    cars_in_traffic
    .groupBy("municipality_code", "year")
    .pivot("ownership_category_code", COUNT_OWNERSHIP_CODES)
    .agg(functions.first("vehicle_count"))
)
checks.rows("cars_in_traffic", "total cars (000) is never missing",
            cars_by_category.filter(functions.col("000").isNull()))
checks.rows("cars_in_traffic", "total (000) = women (010) + men (020) + legal entities (030)",
            cars_by_category.filter(
                functions.col("000") != functions.col("010") + functions.col("020") + functions.col("030")
            ))

# Silver keeps only the ownership codes it knows, so a new SCB category would otherwise be
# dropped without anyone noticing. Checked against bronze, before that filter.
checks.rows("cars_in_traffic", "every ownership category in bronze is a known count or ratio",
            cars_in_traffic_bronze
            .select(functions.col("Agarkategori").alias("ownership_category_code"),
                    functions.col("Agarkategori_label").alias("label"))
            .distinct()
            .filter(~functions.col("ownership_category_code").isin(COUNT_OWNERSHIP_CODES + RATIO_OWNERSHIP_CODES)))
checks.rows("fuel_type", "no new fuel codes; decide whether a new one counts as electrified",
            fuel_type.filter(~functions.col("fuel_type_code").isin(KNOWN_FUEL_CODES)),
            severity="warning")

checks.rows("inhabitants", "an estimate exists for every municipality and year",
            inhabitants.filter(functions.col("inhabitants_estimate").isNull()))
checks.rows("inhabitants", "estimate is plausible (1,000 to 1,500,000)",
            inhabitants.filter(~functions.col("inhabitants_estimate").between(1_000, 1_500_000)))

checks.rows("electricity_prices_daily", "average price within -5 to 20 SEK/kWh",
            daily_prices.filter(~functions.col("avg_sek_per_kwh").between(-5, 20)))
checks.rows("electricity_prices_daily",
            "complete days: 24 hours or 96 quarters (23/25 or 92/100 on DST days)",
            daily_prices.filter(~functions.col("period_count").isin(23, 24, 25, 92, 96, 100)))
# Data errors (e.g. öre instead of SEK) are caught by the error check above. This warning is
# for real but unprecedented prices: the highest daily average since the data starts is
# 4.83 SEK/kWh (2022-12-14, the energy crisis), so 6 flags only something new. Once it fires it
# keeps firing until someone has looked at it and raised the limit — on purpose.
HIGHEST_EXPECTED_DAILY_PRICE = 6
checks.rows("electricity_prices_daily",
            f"daily average above {HIGHEST_EXPECTED_DAILY_PRICE} SEK/kWh, higher than ever seen",
            daily_prices.filter(functions.col("avg_sek_per_kwh") > HIGHEST_EXPECTED_DAILY_PRICE),
            severity="warning")

checks.enforce("before write", "Nothing was written; silver is unchanged since the last good run.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write

# COMMAND ----------

for table_name, (dataframe, key_columns) in silver_tables.items():
    merge_into_silver(dataframe, table_name, key_columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality checks — after writing
# MAGIC
# MAGIC A safety net on the stored tables. With a deduplicated source a `MERGE` cannot create a
# MAGIC duplicate key, so these should never fail; if one does, something outside this notebook
# MAGIC wrote to silver. The runbook covers restoring the table with `RESTORE TABLE`.

# COMMAND ----------

for table_name, (_, key_columns) in silver_tables.items():
    checks.rows(table_name, "no duplicate business keys in the stored table",
                duplicate_keys(spark.table(f"{CATALOG}.{SILVER_SCHEMA}.{table_name}"), key_columns))

checks.enforce(
    "after write",
    "Silver was written and is now inconsistent; see the runbook (RESTORE TABLE) before re-running.",
)
