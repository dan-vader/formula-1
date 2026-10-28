from pyspark.sql import functions as F
from pyspark.sql import DataFrame, Window

from formula_1.silver import config
from formula_1.silver.schemas import openf1 as O
from formula_1.silver.transforms import parse, dedup, snapshot
from formula_1.silver.transforms.parse import _try_cast
from formula_1.silver.transforms.race_control import track_status

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
        (compound.isNull() | compound.isin(COMPOUNDS)).alias("_chk_compound_ok"),
        _try_cast("tyre_age_at_start", "int").alias("tyre_age_at_start"),
        F.col("_ingested_at").alias("ingested_at"),
        "_load_id"
    )


# silver_lap
def lap_parsed() -> DataFrame:
    snap = snapshot.latest_valid_openf1(config.bronze("openf1_laps_raw"))
    return parse.with_stream(
        batch=parse.explode_openf1(snap, O.LAPS),
        endpoint="laps",
        record_ddl=O.LAPS
    )


def lap_filtered(df, sessions) -> DataFrame:
    known = sessions.select(F.col("openf1_session_key").alias("_sk")).distinct()
    return df.join(
        known,
        on=_try_cast("session_key", "int") == known["_sk"],
        how="left_semi"
    )


def lap_deduped(df) -> DataFrame:
    keyed = (
        df.withColumn("_s", _try_cast("session_key", "int"))
            .withColumn("_d", _try_cast("driver_number", "int"))
            .withColumn("_l", _try_cast("lap_number", "int"))
    )
    return dedup.dedup_latest(keyed, ["_s", "_d", "_l"])


def _pit_in_laps() -> DataFrame:
    snap = snapshot.latest_valid_openf1(config.bronze("openf1_pit_raw"))
    parsed_pits = parse.explode_openf1(snap, O.PIT)
    return parsed_pits.select(
        _try_cast("session_key", "int").alias("openf1_session_key"),
        _try_cast("driver_number", "int").alias("driver_number"),
        _try_cast("lap_number", "int").alias("lap_number")
    ).distinct()


def _position_samples() -> DataFrame:
    snap = snapshot.latest_valid_openf1(config.bronze("openf1_position_raw"))
    parsed_positions = parse.explode_openf1(snap, O.POSITION)
    return parsed_positions.select(
        _try_cast("session_key", "int").alias("openf1_session_key"),
        _try_cast("driver_number", "int").alias("driver_number"),
        _try_cast("date", "timestamp").alias("ts"),
        _try_cast("position", "int").alias("pos")
    ).where(F.col("ts").isNotNull())


# Typed lap columns. lap_end_utc = start + duration; without duration - the start of the next lap
def _lap_times(df) -> DataFrame:
    b = df.select(
        _try_cast("session_key", "int").alias("openf1_session_key"),
        _try_cast("driver_number", "int").alias("driver_number"),
        _try_cast("lap_number", "int").alias("lap_number"),
        _try_cast("date_start", "timestamp").alias("lap_start_utc"),
        _try_cast("lap_duration", "double").alias("_dur_s"),
        F.coalesce(F.lower(F.col("is_pit_out_lap")) == "true", F.lit(False)).alias("is_pit_out_lap"),
        F.col("_ingested_at"),
        F.col("_load_id")
    )
    w = Window.partitionBy("openf1_session_key", "driver_number").orderBy("lap_number")
    end = F.coalesce(
        F.expr("lap_start_utc + make_dt_interval(0, 0, 0, try_cast(_dur_s AS DECIMAL(18, 6)))"),
        F.lead("lap_start_utc").over(w)
    )
    return (
        b.withColumn("lap_end_utc", end)
            .withColumn("lap_duration_ms", F.round(F.col("_dur_s") * 1000).cast("bigint"))
            .drop("_dur_s")
    )


# Last known position at lap_end_utc
def _position_at_lap_end(laps, positions) -> DataFrame:
    keys = ["openf1_session_key", "driver_number"]
    a = laps.select(
        *keys,
        "lap_number",
        F.col("lap_end_utc").alias("ts"),
        F.lit(None).cast("int").alias("pos"),
        F.lit(1).alias("_kind")
    )
    b = positions.select(
        *keys,
        F.lit(None).cast("int").alias("lap_number"),
        "ts",
        "pos",
        F.lit(0).alias("_kind")
    )
    w = Window.partitionBy(*keys).orderBy("ts", "_kind").rowsBetween(Window.unboundedPreceding, 0)
    filled = (
        a.unionByName(b)
        .withColumn("position_at_lap_end", F.last("pos", ignorenulls=True).over(w))
    )
    return filled.where("_kind = 1").select("openf1_session_key", "driver_number", "lap_number", "position_at_lap_end")


# Stint of the lap: compound, stint_number, tyre_age_laps
def _lap_stint(laps, stints) -> DataFrame:
    st = stints.select(
        F.col("openf1_session_key").alias("_st_s"),
        F.col("driver_number").alias("_st_d"),
        "stint_number",
        "lap_start",
        "lap_end",
        "compound",
        "tyre_age_at_start"
    )
    keys = ["openf1_session_key", "driver_number", "lap_number"]
    cond = (
        (laps["openf1_session_key"] == st["_st_s"])
        & (laps["driver_number"] == st["_st_d"])
        & (laps["lap_number"] >= st["lap_start"])
        & (laps["lap_number"] <= st["lap_end"])
    )
    w = Window.partitionBy(*keys).orderBy(F.col("stint_number").asc_nulls_last())
    return (
        laps.join(st, cond, "left")
            .drop("_st_s", "_st_d")
            .withColumn("_rn", F.row_number().over(w))
            .where(F.col("_rn") == 1)
            .drop("_rn")
            .withColumn("tyre_age_laps", F.col("tyre_age_at_start") + (F.col("lap_number") - F.col("lap_start")))
    )


# Number of drivers per session
def _driver_count(session_driver) -> DataFrame:
    return (
        session_driver
            .groupBy("openf1_session_key")
            .agg(F.count(F.lit(1)).alias("_chk_n_drivers"))
    )


# stints: silver_stint. session_driver: silver_session_driver. intervals: silver_track_status_interval.
# pit_in: pit_in_laps(). positions: position_samples().
def lap_enriched(df, stints, session_driver, intervals, pit_in, positions) -> DataFrame:
    keys = ["openf1_session_key", "driver_number", "lap_number"]
    drivers = session_driver.select("openf1_session_key", "driver_number", "driver_id", "constructor_id")

    laps = (
        _lap_times(df)
        .join(drivers, ["openf1_session_key", "driver_number"], "left")
        .join(pit_in.withColumn("is_pit_in_lap", F.lit(True)), keys, "left")
        .fillna({"is_pit_in_lap": False})
    )

    laps = _lap_stint(laps, stints)
    laps = track_status(laps, intervals, keys, F.col("lap_start_utc"), F.col("lap_end_utc"))
    laps = (
        laps.join(_position_at_lap_end(laps, positions), keys, "left")
        .join(_driver_count(session_driver), "openf1_session_key", "left")
    )

    return laps.select(
        *keys,
        "driver_id",
        "constructor_id",
        "lap_start_utc",
        "lap_end_utc",
        "lap_duration_ms",
        "is_pit_in_lap",
        "is_pit_out_lap",
        "compound",
        "stint_number",
        "tyre_age_laps",
        "track_status_name",
        "position_at_lap_end",
        F.col("_ingested_at").alias("ingested_at"),
        "_chk_n_drivers",
        "_load_id",
    )
