from pyspark.sql import functions as F
from pyspark.sql import DataFrame, Window

from formula_1.silver import config
from formula_1.silver.transforms import snapshot, parse, dedup
from formula_1.silver.transforms.parse import _try_cast
from formula_1.silver.schemas import openf1 as O
from formula_1.silver.transforms.race_control import track_status

KEYS = ["openf1_session_key", "driver_number", "lap_number"]


# silver_pit_stop
def pit_parsed() -> DataFrame:
    snap = snapshot.latest_valid_openf1(config.bronze("openf1_pit_raw"))
    return parse.explode_openf1(snap, O.PIT)


def pit_filtered(df, sessions) -> DataFrame:
    known = sessions.select(F.col("openf1_session_key").alias("_sk")).distinct()
    return df.join(
        known,
        on=_try_cast("session_key", "int") == known["_sk"],
        how="left_semi"
    )


def pit_deduped(df) -> DataFrame:
    keyed = (
        df.withColumn("_s", _try_cast("session_key", "int"))
            .withColumn("_d", _try_cast("driver_number", "int"))
            .withColumn("_l", _try_cast("lap_number", "int"))
    )
    return dedup.dedup_latest(keyed, ["_s", "_d", "_l"])


# Typed pit columns
def _pit_times(df) -> DataFrame:
    lane_dur_s = F.coalesce(_try_cast("lane_duration", "double"), _try_cast("pit_duration", "double"))
    return df.select(
        _try_cast("session_key", "int").alias("openf1_session_key"),
        _try_cast("driver_number", "int").alias("driver_number"),
        _try_cast("lap_number", "int").alias("lap_number"),
        _try_cast("date", "timestamp").alias("pit_entry_utc"),
        F.round(lane_dur_s * 1000).cast("bigint").alias("pit_lane_duration_ms"),
        F.round(_try_cast("stop_duration", "double") * 1000).cast("bigint").alias("stop_duration_ms"),
        F.col("_ingested_at"),
        F.col("_load_id")
    )


# Tyre fitted: the first non-empty stint that starts on the lap of the visit or the lap after
# Tyre removed: the stint before it (stint_number - 1), even if it is empty (pit lane start)
# Without a new stint the visit changes no tyres (drive-through, penalty): both are NULL.
def _pit_tyres(pits, stints) -> DataFrame:
    new = (
        stints.where(F.col("lap_end") >= F.col("lap_start"))
        .select(
            F.col("openf1_session_key").alias("_sk"),
            F.col("driver_number").alias("_d"),
            F.col("stint_number").alias("_n"),
            F.col("lap_start").alias("_start"),
            F.col("compound").alias("compound_fitted")
        )
    )
    old = stints.select(
        F.col("openf1_session_key").alias("_sk2"),
        F.col("driver_number").alias("_d2"),
        F.col("stint_number").alias("_n2"),
        F.col("compound").alias("compound_removed")
    )

    w = Window.partitionBy(*KEYS).orderBy(F.col("_n"))
    fitted = (
        pits.select(*KEYS)
        .join(
            new,
            on=(
                (F.col("openf1_session_key") == F.col("_sk")) &
                (F.col("driver_number") == F.col("_d")) &
                ((F.col("_start") == F.col("lap_number")) | (F.col("_start") == F.col("lap_number") + 1))
            ),
            how="inner"
        )
        .withColumn("_rn", F.row_number().over(w))
        .where(F.col("_rn") == 1)
        .select(*KEYS, F.col("_n").alias("_fitted_n"), "compound_fitted")
    )
    removed = (
        fitted.join(
            old,
            on=(
                (F.col("openf1_session_key") == F.col("_sk2")) &
                (F.col("driver_number") == F.col("_d2")) &
                (F.col("_n2") == F.col("_fitted_n") - 1)
            ),
            how="inner"
        )
        .select(*KEYS, "compound_removed")
    )
    return (
        pits.join(fitted, KEYS, "left")
        .join(removed, KEYS, "left")
        .withColumn("is_tyre_change", F.col("_fitted_n").isNotNull())
        .drop("_fitted_n")
    )


# Race control writes PENALTY SERVED a few laps after the stop, so it goes to the last visit of the driver before it
def _penalty_visits(race_control, pits) -> DataFrame:
    # Extract driver number from the race control message if it comes as NULL
    driver = F.coalesce(
        F.col("driver_number"),
        F.expr("try_cast(nullif(regexp_extract(message, 'FOR CAR ([0-9]+)', 1), '') AS INT)")
    )
    events = (
        race_control
            .where(F.col("event_type") == "PENALTY_SERVED")
            .select(
                F.col("openf1_session_key").alias("_s"),
                driver.alias("_d"),
                "event_ts"
            )
            .where(F.col("_d").isNotNull())
    )
    visits = pits.select(*KEYS, "pit_entry_utc")

    joined = events.join(
        visits,
        on=((F.col("_s") == visits["openf1_session_key"]) &
        (F.col("_d") == visits["driver_number"]) &
        (visits["pit_entry_utc"] <= F.col("event_ts"))),
        how="inner"
    )

    w = Window.partitionBy("_s", "_d", "event_ts").orderBy(F.col("pit_entry_utc").desc())
    return (
        joined.withColumn("_rn", F.row_number().over(w))
            .where(F.col("_rn") == 1)
            .select(*KEYS)
            .distinct()
            .withColumn("is_penalty_served", F.lit(True))
    )


# Positions before and after the visit
def _pit_positions(pits, lap) -> DataFrame:
    lp = lap.select("openf1_session_key", "driver_number", "lap_number", "position_at_lap_end")
    before = lp.select(
        "openf1_session_key",
        "driver_number",
        (F.col("lap_number") + 1).alias("lap_number"),
        F.col("position_at_lap_end").alias("position_before")
    )
    after = lp.select(
        "openf1_session_key",
        "driver_number",
        (F.col("lap_number") - 1).alias("lap_number"),
        F.col("position_at_lap_end").alias("position_after")
    )
    return pits.join(before, KEYS, "left").join(after, KEYS, "left")


def pit_enriched(df, stints, lap, race_control, session_driver, intervals) -> DataFrame:
    drivers = session_driver.select("openf1_session_key", "driver_number", "driver_id", "constructor_id")

    pits = _pit_times(df).join(drivers, ["openf1_session_key", "driver_number"], "left")
    pits = _pit_tyres(pits, stints)
    pits = (
        pits
        .join(_penalty_visits(race_control, pits), on=KEYS, how="left")
        .fillna({"is_penalty_served": False})
    )
    pits = (
        pits.withColumn(
            "_pit_exit_utc",
            F.expr("pit_entry_utc + make_dt_interval(0, 0, 0, CAST(pit_lane_duration_ms / 1000 AS DECIMAL(18, 6)))")
        )
    )
    pits = track_status(pits, intervals, KEYS, F.col("pit_entry_utc"), F.col("_pit_exit_utc"))
    pits = _pit_positions(pits, lap)

    return pits.select(
        *KEYS,
        "driver_id",
        "constructor_id",
        "pit_entry_utc",
        "pit_lane_duration_ms",
        "stop_duration_ms",
        "compound_removed",
        "compound_fitted",
        "is_tyre_change",
        "is_penalty_served",
        "track_status_name",
        "position_before",
        "position_after",
        (F.col("position_after") - F.col("position_before")).alias("positions_lost"),
        F.col("_ingested_at").alias("ingested_at"),
        "_load_id"
    )


# silver_overtake
def overtake_parsed() -> DataFrame:
    snap = snapshot.latest_valid_openf1(config.bronze("openf1_overtakes_raw"))
    return parse.explode_openf1(snap, O.OVERTAKES)


def overtake_filtered(df, sessions) -> DataFrame:
    known = sessions.select(F.col("openf1_session_key").alias("_sk")).distinct()
    return df.join(
        known,
        on=_try_cast("session_key", "int") == known["_sk"],
        how="left_semi"
    )


def overtake_deduped(df) -> DataFrame:
    keyed = (
        df.withColumn("_sk", _try_cast("session_key", "int"))
            .withColumn("_ts", _try_cast("date", "timestamp"))
            .withColumn("_a", _try_cast("overtaking_driver_number", "int"))
            .withColumn("_b", _try_cast("overtaken_driver_number", "int"))
    )
    return dedup.dedup_latest(keyed, ["_sk", "_ts", "_a", "_b"])


def overtake_enriched(df) -> DataFrame:
    return df.select(
        _try_cast("session_key", "int").alias("openf1_session_key"),
        _try_cast("date", "timestamp").alias("overtake_ts"),
        _try_cast("overtaking_driver_number", "int").alias("overtaking_driver_number"),
        _try_cast("overtaken_driver_number", "int").alias("overtaken_driver_number"),
        _try_cast("position", "int").alias("position"),
        F.col("_ingested_at").alias("ingested_at"),
        "_load_id"
    )
