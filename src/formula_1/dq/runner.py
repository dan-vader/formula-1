"""Runs data-quality checks with Spark and stores the results."""

from datetime import UTC, datetime

from formula_1.logging_utils import get_logger

log = get_logger("formula_1.dq")

RESULTS_SCHEMA = (
    "run_id STRING, checked_at TIMESTAMP, layer STRING, check_group STRING, check_name STRING, "
    "table_name STRING, failed_rows BIGINT, passed BOOLEAN, error STRING"
)


def run_check(spark, check):
    """Runs one check; a query that fails to run counts as a failed check, the run goes on."""
    try:
        failed_rows = spark.sql(check.sql).first()["failed_rows"]
        error = None
    except Exception as exc:  # noqa: BLE001 - any failing query is recorded and the other checks still run
        failed_rows, error = None, str(exc)[:1000]
    passed = error is None and failed_rows == 0
    log.info(
        "%s %s.%s on %s: failed_rows=%s",
        "PASS" if passed else "FAIL",
        check.group,
        check.name,
        check.table,
        failed_rows,
    )
    return {"check": check, "failed_rows": failed_rows, "passed": passed, "error": error}


def run_checks(spark, checks):
    return [run_check(spark, check) for check in checks]


def save_results(spark, results, results_table, run_id, layer):
    checked_at = datetime.now(UTC)
    rows = [
        (
            run_id,
            checked_at,
            layer,
            r["check"].group,
            r["check"].name,
            r["check"].table,
            r["failed_rows"],
            r["passed"],
            r["error"],
        )
        for r in results
    ]
    spark.createDataFrame(rows, RESULTS_SCHEMA).write.mode("append").saveAsTable(results_table)


def failed_checks(results):
    return [r for r in results if not r["passed"]]
