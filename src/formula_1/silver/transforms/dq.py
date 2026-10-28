from importlib.resources import files

import yaml
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

CHECKS = files("formula_1.silver") / "checks"
RESULT_COLS = ["_errors", "_warnings"]


def load_checks(table: str) -> list[dict]:
    return yaml.safe_load((CHECKS / f"{table}.yml").read_text(encoding="utf-8"))


def _engine(df: DataFrame) -> DQEngine:
    return DQEngine(WorkspaceClient(), spark=df.sparkSession)


def apply(df: DataFrame, table: str, ref_dfs: dict | None = None, custom: dict | None = None) -> DataFrame:
    return _engine(df).apply_checks_by_metadata(df, load_checks(table), custom, ref_dfs)


def valid(checked: DataFrame) -> DataFrame:
    return _engine(checked).get_valid(checked)


def quarantine(checked_df, table, key_cols) -> DataFrame:
    payload_cols = [c for c in checked_df.columns if c not in RESULT_COLS]
    failed = F.transform("_errors", lambda e: e["name"])
    key = F.concat_ws("|", *[F.coalesce(F.col(k).cast("string"), F.lit("")) for k in key_cols])
    load_id = F.col("_load_id")
    quarantine_id = F.sha2(
        F.concat_ws("|", F.lit(table), key, F.array_join(failed, ","), F.coalesce(load_id, F.lit(""))),
        256
    )

    return (
        checked_df
            .where(F.col("_errors").isNotNull())
            .select(
                quarantine_id.alias("quarantine_id"),
                F.lit(table).alias("source_table"),
                key.alias("record_key"),
                failed.alias("failed_rules"),
                F.to_json(F.struct(*payload_cols)).alias("payload"),
                load_id.alias("source_load_id"),
                F.col("ingested_at").alias("source_ingested_at")
            )
    )
