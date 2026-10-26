from pyspark.sql import DataFrame, functions as F

from formula_1.gold.config import silver
from formula_1.gold.surrogate import sk, with_inferred_members, with_unknown_member


def dim_session() -> DataFrame:
    known = silver("silver_session").select(
        "session_id",
        "openf1_session_key",
        "season",
        "round",
        "session_type",
        "session_start_utc",
        "event_name",
        "circuit_name",
        "circuit_type",
        "regulation_era",
        F.lit(False).alias("is_inferred"),
        F.lit(None).cast("timestamp").alias("inferred_at"),
    )
    referenced = silver("silver_race_result").groupBy("session_id").agg(
        F.min("snapshot_ts").alias("inferred_at")
    )
    dim = with_inferred_members(
        known,
        referenced,
        "session_id",
        season=F.expr("try_cast(split(session_id, '-')[0] AS int)"),
        round=F.expr("try_cast(split(session_id, '-')[1] AS int)"),
        session_type=F.when(F.split("session_id", "-")[2] == "S", "Sprint").otherwise("Race"),
    )
    columns = [c for c in dim.columns if c != "session_id"]
    return with_unknown_member(dim.select(sk("session_id").alias("session_sk"), *columns), "session_sk")