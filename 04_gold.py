# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Gold
# MAGIC
# MAGIC Star schema plus one denormalised decision table.
# MAGIC
# MAGIC Dimensions and facts are rebuilt in full with `CREATE OR REPLACE TABLE`.
# MAGIC Gold is small and derived entirely from silver, so a deterministic rebuild is
# MAGIC simpler to reason about than incremental logic — and it is the third
# MAGIC idempotency mechanism in the pipeline: bronze skips, silver merges, gold
# MAGIC rebuilds.
# MAGIC
# MAGIC **Order of work: build → check → write**, as in silver. Every table is first defined as a
# MAGIC temporary view, the views are checked, and only then are the tables replaced. A failed
# MAGIC check leaves gold exactly as the last good run left it, so management never reads a
# MAGIC half-built or wrong decision table.
# MAGIC
# MAGIC No date dimension. The facts carry real `DATE` columns (`registration_month`,
# MAGIC `reference_date`, `price_date`) typed in silver, and nothing here needs fiscal
# MAGIC calendars, holidays or week numbers. A date dimension would be ceremony rather than
# MAGIC function at this grain.
# MAGIC
# MAGIC `charging_station_candidates` is the table management reads. It deliberately
# MAGIC contains inputs rather than a ranking score — the weighting between fleet
# MAGIC size, growth and electricity price is a business decision, and hard-coding it
# MAGIC into the pipeline would mean a rebuild every time someone wants to argue about
# MAGIC it.

# COMMAND ----------

# MAGIC %run ./quality_checks

# COMMAND ----------

dbutils.widgets.text("schema_prefix", "", "Schema prefix (e.g. dev_)")
dbutils.widgets.text("run_id", "manual", "Job run id (set by the job)")
SCHEMA_PREFIX = dbutils.widgets.get("schema_prefix")
RUN_ID = dbutils.widgets.get("run_id")

CATALOG = "axenil_assignment1"
SILVER_SCHEMA = f"{SCHEMA_PREFIX}silver"
GOLD_SCHEMA = f"{SCHEMA_PREFIX}gold"
OPS_SCHEMA = f"{SCHEMA_PREFIX}ops"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{OPS_SCHEMA}")

checks = QualityChecks(f"{CATALOG}.{OPS_SCHEMA}.quality_check_results", RUN_ID, "gold")

GOLD_TABLES = [
    "dim_municipality", "dim_fuel_type", "dim_ownership_category",
    "fact_new_registrations", "fact_cars_in_traffic", "fact_inhabitants",
    "fact_electricity_price_daily", "charging_station_candidates",
]

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TEMPORARY VIEW dim_municipality AS
    SELECT
        municipality_code,
        municipality_name,
        county_code,
        county_name,
        price_area,
        price_area_is_split
    FROM {CATALOG}.{SILVER_SCHEMA}.municipality
""")

spark.sql(f"""
    CREATE OR REPLACE TEMPORARY VIEW dim_fuel_type AS
    SELECT
        fuel_type_code,
        fuel_type_name,
        is_electrified
    FROM {CATALOG}.{SILVER_SCHEMA}.fuel_type
""")

spark.sql(f"""
    CREATE OR REPLACE TEMPORARY VIEW dim_ownership_category AS
    SELECT
        ownership_category_code,
        ownership_category_name
    FROM {CATALOG}.{SILVER_SCHEMA}.ownership_category
""")


# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TEMPORARY VIEW fact_new_registrations AS
    SELECT
        municipality_code,
        registration_month,
        fuel_type_code,
        registration_count
    FROM {CATALOG}.{SILVER_SCHEMA}.new_registrations
""")

spark.sql(f"""
    CREATE OR REPLACE TEMPORARY VIEW fact_cars_in_traffic AS
    SELECT
        municipality_code,
        year,
        reference_date,
        ownership_category_code,
        vehicle_count
    FROM {CATALOG}.{SILVER_SCHEMA}.cars_in_traffic
""")

spark.sql(f"""
    CREATE OR REPLACE TEMPORARY VIEW fact_inhabitants AS
    SELECT
        municipality_code,
        year,
        reference_date,
        inhabitants_estimate
    FROM {CATALOG}.{SILVER_SCHEMA}.inhabitants
""")

spark.sql(f"""
    CREATE OR REPLACE TEMPORARY VIEW fact_electricity_price_daily AS
    SELECT
        price_area,
        price_date,
        avg_sek_per_kwh,
        min_sek_per_kwh,
        max_sek_per_kwh,
        resolution_minutes
    FROM {CATALOG}.{SILVER_SCHEMA}.electricity_prices_daily
""")


# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMPORARY VIEW charging_station_candidates AS
WITH latest_vehicle_year AS (
    SELECT MAX(year) AS year
    FROM fact_cars_in_traffic
),
vehicles AS (
    SELECT
        municipality_code,
        MAX(CASE WHEN ownership_category_code = '000' THEN vehicle_count END) AS cars_total,
        MAX(CASE WHEN ownership_category_code = '030' THEN vehicle_count END) AS cars_legal_entities,
        MAX(CASE WHEN ownership_category_code = '040' THEN vehicle_count END) AS cars_taxi
    FROM fact_cars_in_traffic
    WHERE year = (SELECT year FROM latest_vehicle_year)
    GROUP BY municipality_code
),
inhabitants AS (
    SELECT municipality_code, inhabitants_estimate
    FROM fact_inhabitants
    WHERE year = (SELECT year FROM latest_vehicle_year)
),
latest_month AS (
    SELECT MAX(registration_month) AS month
    FROM fact_new_registrations
),
recent_registrations AS (
    -- Last 12 published months, and the 12 months before that for growth.
    SELECT
        municipality_code,
        SUM(CASE WHEN fuel_type_code = '120' AND is_last_12m THEN registration_count ELSE 0 END) AS ev_registrations_12m,
        SUM(CASE WHEN fuel_type_code = '120' AND NOT is_last_12m THEN registration_count ELSE 0 END) AS ev_registrations_prev_12m,
        SUM(CASE WHEN fuel_type_code = '140' AND is_last_12m THEN registration_count ELSE 0 END) AS phev_registrations_12m,
        SUM(CASE WHEN is_last_12m THEN registration_count ELSE 0 END) AS total_registrations_12m
    FROM (
        SELECT
            fact.*,
            fact.registration_month > add_months(latest_month.month, -12) AS is_last_12m
        FROM fact_new_registrations AS fact
        CROSS JOIN latest_month
        WHERE fact.registration_month > add_months(latest_month.month, -24)
    )
    GROUP BY municipality_code
),
electric_stock AS (
    -- Approximation: SCB publishes no fleet figures by fuel type, so the
    -- electric stock is the cumulative sum of electric registrations over the
    -- loaded period. Ignores scrapping, export and moves between
    -- municipalities. Acceptable for electric cars specifically, since
    -- essentially the whole fleet was registered recently.
    SELECT
        municipality_code,
        SUM(registration_count) AS electric_stock_estimate
    FROM fact_new_registrations
    WHERE fuel_type_code = '120'
    GROUP BY municipality_code
),
prices AS (
    SELECT
        price_area,
        ROUND(AVG(avg_sek_per_kwh) * 100, 2) AS avg_ore_per_kwh_12m,
        ROUND(STDDEV(avg_sek_per_kwh) * 100, 2) AS price_stddev_ore_12m
    FROM fact_electricity_price_daily
    WHERE price_date
          > (SELECT MAX(price_date) FROM fact_electricity_price_daily)
            - INTERVAL 365 DAYS
    GROUP BY price_area
)
SELECT
    municipality.municipality_code,
    municipality.municipality_name,
    municipality.county_code,
    municipality.county_name,
    municipality.price_area,
    municipality.price_area_is_split,

    vehicles.cars_total,
    vehicles.cars_legal_entities,
    vehicles.cars_taxi,
    -- Cars are registered at the owner's address, so company and leasing cars land
    -- where the company is registered, not where they are driven. A high share
    -- means the municipality's EV figures are probably inflated by company fleets.
    ROUND(vehicles.cars_legal_entities / NULLIF(vehicles.cars_total, 0), 4) AS legal_entity_share_of_fleet,
    inhabitants.inhabitants_estimate,

    recent_registrations.ev_registrations_12m,
    recent_registrations.ev_registrations_prev_12m,
    ROUND(
        recent_registrations.ev_registrations_12m
        / NULLIF(recent_registrations.ev_registrations_prev_12m, 0) - 1, 4
    ) AS ev_registration_growth,
    -- The absolute change next to the percentage: +200 % can mean 2 -> 6 cars.
    -- Small municipalities are deliberately not filtered out; the reader sees both.
    recent_registrations.ev_registrations_12m
        - recent_registrations.ev_registrations_prev_12m AS ev_registrations_change,
    recent_registrations.phev_registrations_12m,
    recent_registrations.total_registrations_12m,
    ROUND(
        recent_registrations.ev_registrations_12m
        / NULLIF(recent_registrations.total_registrations_12m, 0), 4
    ) AS ev_share_of_new_registrations,

    electric_stock.electric_stock_estimate,
    ROUND(
        electric_stock.electric_stock_estimate
        / NULLIF(vehicles.cars_total, 0), 4
    ) AS electric_share_of_fleet,
    ROUND(
        electric_stock.electric_stock_estimate * 1000
        / NULLIF(inhabitants.inhabitants_estimate, 0), 1
    ) AS electric_cars_per_1000_inhabitants,

    prices.avg_ore_per_kwh_12m,
    prices.price_stddev_ore_12m,

    (SELECT month FROM latest_month) AS registrations_through_month,
    (SELECT year FROM latest_vehicle_year) AS vehicles_as_of_year

FROM dim_municipality AS municipality
LEFT JOIN vehicles              ON vehicles.municipality_code = municipality.municipality_code
LEFT JOIN inhabitants           ON inhabitants.municipality_code = municipality.municipality_code
LEFT JOIN recent_registrations  ON recent_registrations.municipality_code = municipality.municipality_code
LEFT JOIN electric_stock        ON electric_stock.municipality_code = municipality.municipality_code
LEFT JOIN prices                ON prices.price_area = municipality.price_area
""")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality checks — before writing
# MAGIC
# MAGIC Besides shape and completeness, gold checks **freshness**. A source that silently stops
# MAGIC publishing makes every other check pass while the decision table quietly ages; these
# MAGIC checks turn that into a failed run.

# COMMAND ----------

from pyspark.sql import functions

candidates = spark.table("charging_station_candidates")
EXPECTED_MUNICIPALITY_COUNT = 290

candidate_count = candidates.count()
checks.condition(
    "charging_station_candidates", f"one row per municipality ({EXPECTED_MUNICIPALITY_COUNT})",
    candidate_count == EXPECTED_MUNICIPALITY_COUNT, f"found {candidate_count}",
)
checks.rows("charging_station_candidates", "municipality_code is unique",
            duplicate_keys(candidates, ["municipality_code"]))
checks.rows("charging_station_candidates", "every municipality has cars, inhabitants, registrations and a price",
            candidates.filter(
                functions.col("cars_total").isNull()
                | functions.col("inhabitants_estimate").isNull()
                | functions.col("ev_registrations_12m").isNull()
                | functions.col("avg_ore_per_kwh_12m").isNull()
            ).select("municipality_code", "municipality_name", "cars_total", "inhabitants_estimate",
                     "ev_registrations_12m", "avg_ore_per_kwh_12m"))
checks.rows("charging_station_candidates", "estimated electric share of the fleet above 100 %",
            candidates.filter(functions.col("electric_share_of_fleet") > 1)
            .select("municipality_name", "electric_share_of_fleet", "legal_entity_share_of_fleet"),
            severity="warning")

freshness = spark.sql("""
    SELECT
        (SELECT MAX(registration_month) FROM fact_new_registrations) AS latest_registration_month,
        (SELECT MAX(year) FROM fact_cars_in_traffic) AS latest_vehicle_year,
        (SELECT MAX(price_date) FROM fact_electricity_price_daily) AS latest_price_date,
        current_date() AS today
""").first()

# SCB publishes registrations about a month after the month ends, so four months behind means
# several missed publications. Cars in traffic for a year is published in February the year
# after. Electricity prices are published daily.
months_behind = (freshness["today"].year - freshness["latest_registration_month"].year) * 12 \
    + freshness["today"].month - freshness["latest_registration_month"].month
checks.condition("fact_new_registrations", "registrations at most 4 months old",
                 months_behind <= 4, f"latest month is {freshness['latest_registration_month']}")
checks.condition("fact_cars_in_traffic", "cars in traffic at most 2 years old",
                 freshness["today"].year - freshness["latest_vehicle_year"] <= 2,
                 f"latest year is {freshness['latest_vehicle_year']}")
checks.condition("fact_electricity_price_daily", "electricity prices at most 3 days old",
                 (freshness["today"] - freshness["latest_price_date"]).days <= 3,
                 f"latest day is {freshness['latest_price_date']}")

checks.enforce("before write", "Nothing was written; gold is unchanged since the last good run.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write

# COMMAND ----------

for table_name in GOLD_TABLES:
    spark.sql(f"CREATE OR REPLACE TABLE {CATALOG}.{GOLD_SCHEMA}.{table_name} AS SELECT * FROM {table_name}")
    print(table_name, spark.table(f"{CATALOG}.{GOLD_SCHEMA}.{table_name}").count())

# COMMAND ----------

display(
    spark.table(f"{CATALOG}.{GOLD_SCHEMA}.charging_station_candidates")
    .orderBy(functions.col("electric_stock_estimate").desc())
    .limit(25)
)
