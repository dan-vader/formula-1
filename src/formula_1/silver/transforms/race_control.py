from pyspark.sql import functions as F
from pyspark.sql import DataFrame, Column, Window

from formula_1.silver import config
from formula_1.silver.transforms import parse, snapshot, dedup
from formula_1.silver.transforms.parse import _try_cast
from formula_1.silver.schemas import openf1 as O


OPENING = ["SAFETY_CAR_DEPLOYED", "VSC_DEPLOYED", "RED_FLAG"]
CLOSING = ["SAFETY_CAR_IN", "VSC_ENDING", "TRACK_CLEAR"]
SEVERITY = {"VSC_DEPLOYED": 1, "SAFETY_CAR_DEPLOYED": 2, "RED_FLAG": 3}
TRACK_NAME = {1: "Virtual Safety Car", 2: "Safety Car", 3: "Red Flag"}


def _by_value(col, mapping, default=None) -> Column:
    return F.coalesce(*[F.when(col == k, F.lit(v)) for k, v in mapping.items()], F.lit(default))


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


# silver_track_status_interval
# Each opening event runs until the next closing event of the same session, or until the end of the session
def track_status_intervals(race_control, session) -> DataFrame:
    opening = race_control.where(F.col("event_type").isin(OPENING)).select(
        F.col("openf1_session_key").alias("_o_session"),
        F.col("event_ts").alias("_o_ts"),
        F.col("event_type").alias("_o_type"),
        F.col("ingested_at").alias("_o_ingested")
    )
    closing = race_control.where(F.col("event_type").isin(CLOSING)).select(
        F.col("openf1_session_key").alias("_c_session"),
        F.col("event_ts").alias("_c_ts"),
        F.col("ingested_at").alias("_c_ingested")
    )
    pairs = opening.join(
        closing,
        on=(F.col("_o_session") == F.col("_c_session")) & (F.col("_o_ts") < F.col("_c_ts")),
        how="left"
    )
    rank = _by_value(F.col("_o_type"), SEVERITY)
    w = Window.partitionBy("_o_session", "_o_ts").orderBy(F.col("_c_ts").asc_nulls_last(), rank.desc())
    first_closing = pairs.withColumn("_rn", F.row_number().over(w)).where("_rn = 1")

    session_end = session.select(
        F.col("openf1_session_key").alias("_o_session"),
        "session_end_utc"
    )
    joined = first_closing.join(session_end, "_o_session", "left")
    return joined.select(
        F.col("_o_session").alias("openf1_session_key"),
        F.col("_o_ts").alias("interval_start_utc"),
        F.coalesce("_c_ts", "session_end_utc").alias("interval_end_utc"),
        _by_value(rank, TRACK_NAME).alias("track_status_name"),
        rank.alias("severity_rank"),
        F.greatest("_o_ingested", F.coalesce("_c_ingested", "_o_ingested")).alias("ingested_at"),
        F.lit(None).cast("string").alias("_load_id")
    )

# Adds track_status_name: the most severe interval that overlaps [start, end], else 'Green'
def track_status(df, intervals, keys, start, end) -> DataFrame:
    iv = intervals.select(
        F.col("openf1_session_key").alias("_iv_session"),
        F.col("interval_start_utc").alias("_iv_from"),
        F.col("interval_end_utc").alias("_iv_to"),
        F.col("severity_rank").alias("_iv_rank")
    )
    overlaps = (
        df.select(*keys, start.alias("_from"), end.alias("_to"))
        .join(
            iv,
            on=((F.col("openf1_session_key") == F.col("_iv_session"))
                & (F.col("_iv_from") <= F.col("_to"))
                & (F.col("_iv_to") >= F.col("_from"))),
            how="inner"
        )
        .groupBy(*keys)
        .agg(F.max("_iv_rank").alias("_rank"))
    )
    status = _by_value(F.col("_rank"), TRACK_NAME, default="Green")
    return (
        df.join(overlaps, keys, "left")
            .withColumn("track_status_name", status)
            .drop("_rank")
    )
