from pyspark.sql import functions as F
from pyspark.sql import DataFrame

from formula_1.silver import config
from formula_1.silver.schemas import openf1 as O
from formula_1.silver.transforms import parse, dedup, snapshot
from formula_1.silver.transforms.parse import _try_cast

COMPOUNDS = ["SOFT", "MEDIUM", "HARD", "INTERMEDIATE", "WET"]


# silver_stint
def stint_parsed() -> DataFrame:
    snap = snapshot.latest_valid_openf1(config.bronze("openf1_stints_raw"))
    return parse.explode_openf1(snap, O.STINTS)


def stint_filtered(df, sessions) -> DataFrame:
    known = sessions.select(F.col("openf1_session_key").alias("_sk")).distinct()
    return df.join(
        known,
        on=_try_cast("session_key", "int") == known["_sk"],
        how="left_semi"
    )


def stint_deduped(df) -> DataFrame:
    keyed = (
        df.withColumn("_s", _try_cast("session_key", "int"))
        .withColumn("_d", _try_cast("driver_number", "int"))
        .withColumn("_i", _try_cast("stint_number", "int"))
    )
    return dedup.dedup_latest(keyed, ["_s", "_d", "_i"])


def stint_enriched(df) -> DataFrame:
    compound = F.upper(F.trim(F.col("compound")))
    return df.select(
        _try_cast("session_key", "int").alias("openf1_session_key"),
        _try_cast("driver_number", "int").alias("driver_number"),
        _try_cast("stint_number", "int").alias("stint_number"),
        _try_cast("lap_start", "int").alias("lap_start"),
        _try_cast("lap_end", "int").alias("lap_end"),
        F.when(compound.isin(COMPOUNDS), compound).alias("compound"),
        (compound.isNull() | compound.isin(COMPOUNDS)).alias("_chk_compound_ok").alias("_chk_compound_ok"),
        _try_cast("tyre_age_at_start", "int").alias("tyre_age_at_start"),
        F.col("_ingested_at").alias("ingested_at"),
        "_load_id"
    )
