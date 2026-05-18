#This is to impliment my dqm_script with an PostgreSQL db/dbs
import uuid
import json
import re
import os
import logging
from dotenv import load_dotenv
from contextlib import contextmanager
from datetime import datetime

import psycopg2
import psycopg2.extras
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType, LongType

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Criticality levels
# ------------------------------------------------------------------------
SOFT_FAIL = 0
HARD_FAIL = 1

load_dotenv()
def _get_required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise EnvironmentError(
            f"Required environment variable '{name}' is not set. "
            "Set REDSHIFT_HOST, REDSHIFT_PORT, REDSHIFT_DATABASE, "
            "REDSHIFT_USER, REDSHIFT_PASSWORD before running."
        )
    return value


def get_db_conn() -> psycopg2.extensions.connection:
    """Open and return a psycopg2 connection to Redshift.

    SSL is required — Redshift rejects unencrypted connections by default.
    Port defaults to 5439 (Redshift standard) when REDSHIFT_PORT is not set.
    """
    return psycopg2.connect(
        host=_get_required_env("REDSHIFT_HOST"),
        port=int(os.environ.get("REDSHIFT_PORT", "5439")),
        dbname=_get_required_env("REDSHIFT_DATABASE"),
        user=_get_required_env("REDSHIFT_USER"),
        password=_get_required_env("REDSHIFT_PASSWORD"),
        sslmode="require",          # Redshift requires SSL
        connect_timeout=30,         # fail fast rather than hanging indefinitely
    )


@contextmanager
def db_cursor(conn: psycopg2.extensions.connection):
    """Yield a RealDictCursor; rolls back on error so the connection stays usable."""
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


# ---------------------------------------------------------------------------
# Spark helpers
# ---------------------------------------------------------------------------

def get_spark() -> SparkSession:
    return SparkSession.builder.getOrCreate()


def generate_batch_id() -> int:
    return uuid.uuid4().int & 0x7FFFFFFFFFFFFFFF


# ---------------------------------------------------------------------------
# Data-loading via psycopg2  (replaces all spark.table() reads)
# ---------------------------------------------------------------------------

def load_config(conn: psycopg2.extensions.connection, schema: str, table: str) -> list[dict]:
    """
    Load active rows from ctrl_dqm_master.
    Returns a list of RealDictRow objects (behave like plain dicts).
    Raises RuntimeError when no active configs exist.
    """
    sql = f'SELECT * FROM "{schema}"."{table}" WHERE active = TRUE'
    with db_cursor(conn) as cur:
        cur.execute(sql)
        rows = cur.fetchall()

    if not rows:
        raise RuntimeError(f"No active configs found in {schema}.{table}")

    log.info("Loaded %d active config row(s) from %s.%s", len(rows), schema, table)
    return rows


def load_checks(conn: psycopg2.extensions.connection, schema: str, table: str) -> list[dict]:
    """
    Load all rows from ctrl_dqm_type.
    Returns a list of RealDictRow objects keyed by check_id for O(1) lookup.
    """
    sql = f'SELECT * FROM "{schema}"."{table}"'
    with db_cursor(conn) as cur:
        cur.execute(sql)
        rows = cur.fetchall()

    log.info("Loaded %d check definition(s) from %s.%s", len(rows), schema, table)
    return rows


def load_source(conn: psycopg2.extensions.connection, schema: str, table: str) -> list[dict]:
    """
    Load deduplicated source records from Redshift.

    DISTINCT ON is PostgreSQL-specific and not supported by Redshift.
    Deduplication uses ROW_NUMBER() instead, keeping the first row per
    record_id (arbitrary but deterministic within a single run).
    Returns a list of RealDictRow objects.
    """
    sql = f"""
        SELECT *
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY record_id ORDER BY record_id) AS _rn
            FROM "{schema}"."{table}"
        ) AS _deduped
        WHERE _rn = 1
    """
    with db_cursor(conn) as cur:
        cur.execute(sql)
        rows = cur.fetchall()

    if not rows:
        raise RuntimeError(f"Source table {schema}.{table} is empty or does not exist.")

    # Drop the internal dedup column before handing rows to Spark
    clean_rows = [{k: v for k, v in dict(r).items() if k != "_rn"} for r in rows]

    log.info("Loaded %d source record(s) from %s.%s", len(clean_rows), schema, table)
    return clean_rows


# ---------------------------------------------------------------------------
# Build an in-memory Spark DataFrame from the psycopg2 result
# (needed so we can keep Spark SQL for validation queries and Delta writes)
# ---------------------------------------------------------------------------

def source_records_to_spark_df(spark: SparkSession, records: list[dict]) -> DataFrame:
    """
    Convert a list of dicts (psycopg2 RealDictRow) to a Spark DataFrame.
    Schema is inferred automatically by Spark from the first row.
    """
    if not records:
        raise ValueError("Cannot build a Spark DataFrame from an empty record list.")

    # Convert RealDictRow → plain dict so Spark can serialise them
    plain_dicts = [dict(r) for r in records]
    return spark.createDataFrame(plain_dicts)


# ---------------------------------------------------------------------------
# Schema helper for the log table (still read from Delta via Spark)
# ---------------------------------------------------------------------------

def get_table_schema(spark: SparkSession, table_name: str) -> StructType:
    full_schema = spark.table(table_name).schema
    return StructType([f for f in full_schema.fields if f.name != "log_id"])


# ---------------------------------------------------------------------------
# Core check execution
# ---------------------------------------------------------------------------

def _build_checks_index(checks: list[dict]) -> dict[int, dict]:
    """Index check definitions by check_id for fast lookup."""
    return {int(row["check_id"]): dict(row) for row in checks}


def run_single_check(
    spark: SparkSession,
    source_df: DataFrame,
    config_row: dict,
    checks_index: dict[int, dict],
    batch_id: int,
    run_timestamp: datetime,
    quarantine_table: str,
    passed_table: str,
):
    """
    Execute one DQM check.

    Returns
    -------
    (log_record | None, hard_fail_ids_df, soft_fail_ids_df)
    """
    record_id_field = source_df.schema["record_id"]
    empty_ids = spark.createDataFrame([], StructType([record_id_field]))

    check_id      = int(config_row["check_id"])
    target_column = str(config_row["target_column"])
    target_table  = str(config_row["target_table"])
    criticality   = int(config_row["criticality"])
    threshold     = float(config_row["threshold"]) if config_row.get("threshold") is not None else 0.0

    check_def = checks_index.get(check_id)
    if not check_def:
        log.warning("check_id=%d not found in checks table – skipping.", check_id)
        return None, empty_ids, empty_ids

    query_template: str = check_def["query_template"]

    start_time = datetime.now()
    source_df.createOrReplaceTempView("source_table")

    # Fill in standard placeholders
    validation_query = (
        query_template
        .replace("{table_name}", "source_table")
        .replace("{column_name}", target_column)
    )

    # Inject dynamic params from check_params if present
    raw_params = config_row.get("check_params")
    if raw_params:
        try:
            check_params = json.loads(raw_params)
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"Invalid JSON in check_params for check_id={check_id}: {raw_params!r}"
            ) from exc

        for param_key, param_value in check_params.items():
            placeholder = "{" + param_key + "}"
            if placeholder in validation_query:
                validation_query = validation_query.replace(placeholder, str(param_value))

    # Guard: catch any remaining unfilled placeholders
    unfilled = re.findall(r"\{(\w+)\}", validation_query)
    if unfilled:
        raise ValueError(
            f"check_id={check_id} has unfilled placeholders {unfilled}. "
            "Add these keys to check_params in ctrl_dqm_master."
        )

    log.debug("check_id=%d  query: %s", check_id, validation_query)

    failed_record_ids = spark.sql(validation_query).select("record_id").distinct()

    total_rows    = source_df.count()
    failed_count  = failed_record_ids.count()
    passed_count  = total_rows - failed_count
    execution_time = (datetime.now() - start_time).total_seconds()

    fail_pct = (failed_count / total_rows * 100) if total_rows > 0 else 0.0
    status   = "FAIL" if fail_pct > threshold else "PASS"

    log.info(
        "check_id=%d  status=%s  failed=%d/%d (%.2f%%)  threshold=%.2f%%",
        check_id, status, failed_count, total_rows, fail_pct, threshold,
    )

    log_record = {
        "batch_id":        int(batch_id),
        "check_id":        check_id,
        "target_table":    target_table,
        "target_column":   target_column,
        "total_rows":      int(total_rows),
        "passed_rows":     int(passed_count),
        "failed_rows":     int(failed_count),
        "threshold":       threshold,
        "status":          status,
        "criticality":     criticality,
        "run_timestamp":   run_timestamp,
        "execution_time":  float(execution_time),
        "quarantine_table": quarantine_table,
        "passed_table":    passed_table,
    }

    hard_ids = failed_record_ids if criticality == HARD_FAIL else empty_ids
    soft_ids = failed_record_ids if criticality == SOFT_FAIL else empty_ids
    return log_record, hard_ids, soft_ids


# ---------------------------------------------------------------------------
# Record classification
# ---------------------------------------------------------------------------

def classify_records(spark: SparkSession, source_df: DataFrame, hard_ids: DataFrame, soft_ids: DataFrame):
    """
    Partition source records into passed and quarantined sets.

    Rules
    -----
    - Hard-fail   → always quarantined
    - Soft-fail only (not also hard-fail) → passed
    - Clean (no failure at all) → passed
    """
    pure_soft_ids  = soft_ids.subtract(hard_ids)
    all_failed_ids = hard_ids.union(soft_ids).distinct()
    clean_ids      = source_df.select("record_id").subtract(all_failed_ids)

    quarantined_df = (
        source_df.join(hard_ids, on="record_id", how="inner")
                 .dropDuplicates(["record_id"])
    )
    passed_ids = clean_ids.union(pure_soft_ids).distinct()
    passed_df  = (
        source_df.join(passed_ids, on="record_id", how="inner")
                 .dropDuplicates(["record_id"])
    )
    return passed_df, quarantined_df


# ---------------------------------------------------------------------------
# Persist results  (Spark Delta writes – unchanged)
# ---------------------------------------------------------------------------

def persist_results(
    spark: SparkSession,
    log_records: list[dict],
    dqm_logs_table: str,
    passed_df: DataFrame,
    passed_table: str,
    quarantined_df: DataFrame,
    quarantine_table: str,
):
    if log_records:
        log_schema = get_table_schema(spark, dqm_logs_table)
        logs_df = spark.createDataFrame(log_records, schema=log_schema)
        logs_df.write.format("delta").mode("append").saveAsTable(dqm_logs_table)
        log.info("Wrote %d log record(s) to %s", len(log_records), dqm_logs_table)

    passed_df.write.format("delta").mode("overwrite").saveAsTable(passed_table)
    log.info("Wrote %d passed record(s) to %s", passed_df.count(), passed_table)

    quarantined_df.write.format("delta").mode("overwrite").saveAsTable(quarantine_table)
    log.info("Wrote %d quarantined record(s) to %s", quarantined_df.count(), quarantine_table)


# ---------------------------------------------------------------------------
# Pipeline entry point
# ---------------------------------------------------------------------------

def run_dqm_pipeline(
    catalog: str,
    schema: str,
    ctrl_dqm_master: str,
    ctrl_dqm_type: str,
    source: str,
    quarantine_table: str,
    passed_table: str,
    log_table: str,
) -> dict:
    """
    Orchestrate the full DQM pipeline.

    Source data and control tables are read from Redshift via psycopg2.
    Validation logic runs in Spark (SQL over in-memory temp views).
    Results are written to Delta tables via Spark.
    """
    ns = f"{catalog}.{schema}"
    spark = get_spark()
    batch_id      = generate_batch_id()
    run_timestamp = datetime.now()

    log.info("Starting DQM pipeline  batch_id=%d", batch_id)

    conn = get_db_conn()
    try:
        # ── 1. Read control tables and source data from Redshift ───────────
        config_rows  = load_config(conn, schema, ctrl_dqm_master)   # list[dict]
        checks_rows  = load_checks(conn, schema, ctrl_dqm_type)     # list[dict]
        source_rows  = load_source(conn, schema, source)            # list[dict]
    finally:
        conn.close()
        log.info("Redshift connection closed.")

    # ── 2. Convert source records to a Spark DataFrame ──────────────────────
    source_df     = source_records_to_spark_df(spark, source_rows)
    checks_index  = _build_checks_index(checks_rows)

    record_id_field = source_df.schema["record_id"]
    hard_ids = spark.createDataFrame([], StructType([record_id_field]))
    soft_ids = hard_ids
    log_records: list[dict] = []

    # ── 3. Run each check ────────────────────────────────────────────────────
    for config_row in config_rows:
        log_record, r_hard, r_soft = run_single_check(
            spark, source_df, config_row, checks_index,
            batch_id, run_timestamp,
            f"{ns}.{quarantine_table}",
            f"{ns}.{passed_table}",
        )
        if log_record:
            log_records.append(log_record)
            hard_ids = hard_ids.union(r_hard).distinct()
            soft_ids = soft_ids.union(r_soft).distinct()

    # ── 4. Classify and persist ──────────────────────────────────────────────
    passed_df, quarantined_df = classify_records(spark, source_df, hard_ids, soft_ids)

    persist_results(
        spark, log_records,
        f"{ns}.{log_table}",
        passed_df,      f"{ns}.{passed_table}",
        quarantined_df, f"{ns}.{quarantine_table}",
    )

    result = {
        "batch_id":    batch_id,
        "passed":      passed_df.count(),
        "quarantined": quarantined_df.count(),
    }
    log.info("DQM pipeline complete  %s", result)
    return result


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    _get_required_env()
    
    # run_dqm_pipeline(
    #     catalog         = "com_edp_dev",
    #     schema          = "com_raw",
    #     ctrl_dqm_master = "ctrl_dqm_master",
    #     ctrl_dqm_type   = "ctrl_dqm_type",
    #     source          = "dqm_staging",
    #     quarantine_table= "dqm_quarantined_records",
    #     passed_table    = "dqm_passed_records",
    #     log_table       = "dqm_log",
    # )