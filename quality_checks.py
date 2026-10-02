# Databricks notebook source
# MAGIC %md
# MAGIC # Quality checks (shared)
# MAGIC
# MAGIC Included from `03_silver` and `04_gold` with `%run ./quality_checks`.
# MAGIC
# MAGIC A check is a DataFrame of **offending rows** — empty means passed. Each check has a
# MAGIC severity:
# MAGIC
# MAGIC - `error` — the data is wrong. `enforce()` stops the run.
# MAGIC - `warning` — worth a human look, but not necessarily wrong (an extreme value, say).
# MAGIC   Logged only.
# MAGIC
# MAGIC Every result, passed or not, is appended to `ops.quality_check_results` with the job run
# MAGIC id, so there is a history of every check and a first place to look when a run fails.
# MAGIC
# MAGIC The layers call `enforce()` **before** writing: data is built, checked, and only written
# MAGIC if no error-level check failed. A failed run therefore leaves the tables exactly as the
# MAGIC last good run left them — there is nothing to clean up, only the cause to fix.

# COMMAND ----------

import json

from pyspark.sql import functions
from pyspark.sql.types import BooleanType, LongType, StringType, StructField, StructType

QUALITY_RESULT_SCHEMA = StructType([
    StructField("run_id", StringType(), False),
    StructField("layer", StringType(), False),
    StructField("stage", StringType(), False),
    StructField("table_name", StringType(), False),
    StructField("check_name", StringType(), False),
    StructField("severity", StringType(), False),
    StructField("passed", BooleanType(), False),
    StructField("failed_rows", LongType(), False),
    StructField("sample", StringType(), True),
])


class QualityChecks:
    """Collects check results for one stage of a notebook run, logs them, stops on errors."""

    def __init__(self, results_table, run_id, layer):
        self.results_table = results_table
        self.run_id = run_id
        self.layer = layer
        self.results = []

    def rows(self, table_name, check_name, offending, severity="error"):
        """Passes when the DataFrame of offending rows is empty."""
        sample = offending.limit(5).collect()
        failed_rows = offending.count() if sample else 0
        detail = json.dumps([row.asDict() for row in sample], default=str, ensure_ascii=False) if sample else None
        self._record(table_name, check_name, severity, failed_rows, detail)

    def condition(self, table_name, check_name, is_ok, detail, severity="error"):
        """For checks that are a single fact rather than a set of rows, e.g. a row count."""
        self._record(table_name, check_name, severity, 0 if is_ok else 1, None if is_ok else detail)

    def _record(self, table_name, check_name, severity, failed_rows, detail):
        passed = failed_rows == 0
        self.results.append((table_name, check_name, severity, passed, failed_rows, detail))
        status = "OK  " if passed else ("FAIL" if severity == "error" else "WARN")
        print(f"{status} [{table_name}] {check_name}" + ("" if passed else f" — {failed_rows} rows, e.g. {detail}"))

    def enforce(self, stage, consequence):
        """Log every result collected since the last call, then raise if an error-level check failed.

        stage names where in the run the checks ran ("before write" / "after write");
        consequence is put in the error message so whoever reads it knows the state of the data.
        """
        rows = [
            (self.run_id, self.layer, stage, table_name, check_name, severity, passed, failed_rows, detail)
            for table_name, check_name, severity, passed, failed_rows, detail in self.results
        ]
        if rows:
            (
                spark.createDataFrame(rows, QUALITY_RESULT_SCHEMA)
                .withColumn("checked_at", functions.current_timestamp())
                .write.mode("append")
                .saveAsTable(self.results_table)
            )

        errors = [result for result in self.results if not result[3] and result[2] == "error"]
        self.results = []
        if errors:
            raise RuntimeError(
                f"{self.layer} ({stage}): {len(errors)} quality check(s) failed. {consequence}\n"
                + "\n".join(f"- [{table}] {name}: {detail}" for table, name, _, _, _, detail in errors)
                + f"\nAll results: SELECT * FROM {self.results_table} WHERE run_id = '{self.run_id}'"
            )


def duplicate_keys(dataframe, key_columns):
    """Offending rows for a uniqueness check: keys that occur more than once."""
    return dataframe.groupBy(*key_columns).count().filter(functions.col("count") > 1)
