from pyspark.sql import functions as F
from pyspark.sql import DataFrame, Column

from formula_1.silver import config
from formula_1.silver.transforms import parse, snapshot, dedup
from formula_1.silver.transforms.parse import _try_cast
from formula_1.silver.schemas import openf1 as O


def event_type(category: Column, flag: Column, message: Column) -> Column:
    m = F.upper(message)
    return (
        F.when(m.rlike("^(VIRTUAL SAFETY CAR|VSC) DEPLOYED"), "VSC_DEPLOYED")
        .when(m.rlike("^(VIRTUAL SAFETY CAR|VSC) ENDING"), "VSC_ENDING")
        .when((F.upper(category) == "SAFETYCAR") & m.rlike("DEPLOYED"), "SAFETY_CAR_DEPLOYED")
        .when((F.upper(category) == "SAFETYCAR") & m.rlike("IN THIS LAP"), "SAFETY_CAR_IN")
        .when(F.upper(flag) == "RED", "RED_FLAG")
        .when(m.rlike("^TRACK CLEAR"), "TRACK_CLEAR")
        .when(m.rlike("PENALTY SERVED"), "PENALTY_SERVED")
        .otherwise("OTHER")
    )


# silver_race_control
# TODO: union with the OpenF1 streaming data
def race_control_parsed() -> DataFrame:
    snap = snapshot.latest_valid_openf1(config.bronze("openf1_race_control_raw"))
    return parse.explode_openf1(snap, O.RACE_CONTROL)


def race_control_filtered(df, session) -> DataFrame:
    known = session.select(F.col("openf1_session_key").alias("_sk")).distinct()
    return df.join(
        known,
        on=_try_cast("session_key", "int") == known["_sk"],
        how="left_semi"
    )


def race_control_deduped(df) -> DataFrame:
    hashed = (
        df.withColumn("_event_hash",
            F.sha2(F.concat_ws("|", "session_key", "date", "category", "flag", "scope", "driver_number", "message"),
                    256)
        )
        .withColumn("_s", _try_cast("session_key", "int"))
        .withColumn("_ts", _try_cast("date", "timestamp"))
    )
    return dedup.dedup_latest(hashed, ["_s", "_ts", "_event_hash"])


def race_control_enriched(df) -> DataFrame:
    message = F.trim("message")
    return df.select(
        _try_cast("session_key", "int").alias("openf1_session_key"),
        _try_cast("date", "timestamp").alias("event_ts"),
        F.col("_event_hash").alias("event_hash"),
        F.trim("category").alias("category"),
        F.upper(F.trim("flag")).alias("flag"),
        F.trim("scope").alias("scope"),
        message.alias("message"),
        _try_cast("driver_number", "int").alias("driver_number"),
        _try_cast("lap_number", "int").alias("lap_number"),
        event_type(F.col("category"), F.col("flag"), message).alias("event_type"),
        F.col("_ingested_at").alias("ingested_at"),
        "_load_id"
    )

