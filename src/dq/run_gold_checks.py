import argparse
from datetime import UTC, datetime

from pyspark.sql import SparkSession

from formula_1.dq import runner
from formula_1.dq.gold_checks import DEFAULT_MAX_UNKNOWN_SHARE, gold_checks
from formula_1.logging_utils import get_logger

log = get_logger("formula_1.run_gold_checks")


def parse_args():
    parser = argparse.ArgumentParser(description="Run data-quality checks on the gold tables")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--results-table", required=True)
    parser.add_argument("--max-unknown-share", type=float, default=DEFAULT_MAX_UNKNOWN_SHARE)
    parser.add_argument("--run-id", default=None)
    return parser.parse_args()


def main():
    args = parse_args()
    run_id = args.run_id or datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    spark = SparkSession.builder.getOrCreate()

    checks = gold_checks(args.catalog, args.schema, args.max_unknown_share)
    log.info("Running %d gold checks, run_id=%s", len(checks), run_id)
    results = runner.run_checks(spark, checks)
    runner.save_results(spark, results, args.results_table, run_id, layer="gold")

    failed = runner.failed_checks(results)
    warned = runner.warned_checks(results)
    log.info("Checks: %d total, %d errors, %d warnings", len(results), len(failed), len(warned))
    for r in warned:
        log.warning("WARN %s on %s: failed_rows=%s", r["check"].name, r["check"].table, r["failed_rows"])
    if failed:
        names = ", ".join(f"{r['check'].name} on {r['check'].table}" for r in failed)
        raise RuntimeError(f"{len(failed)} gold checks failed: {names}")


if __name__ == "__main__":
    main()