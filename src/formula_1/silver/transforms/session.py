from pyspark.sql import functions as F
from pyspark.sql import DataFrame, Window

from formula_1.silver import config
from formula_1.silver.schemas import jolpica as J, openf1 as O
from formula_1.silver.transforms import dedup, parse, snapshot
from formula_1.silver.transforms.parse import _try_cast

SESSION_TYPES = ["Race", "Sprint"]


# session_id: 2026-04-R
def session_id(season, round, session_type):
    return F.concat_ws("-",
        season.cast("string"),
        F.lpad(round.cast("string"), 2, "0"),
        F.when(session_type == "Race", "R").otherwise("S")
    )


def session_parsed() -> DataFrame:
    snap = snapshot.latest_valid_openf1(config.bronze("openf1_sessions_raw"))
    return parse.explode_openf1(snap, O.SESSIONS)


def session_filtered(df) -> DataFrame:
    is_cancelled = F.lower(F.coalesce(F.col("is_cancelled"), F.lit("false"))) == "true"
    return df.where(F.col("session_name").isin(SESSION_TYPES) & ~is_cancelled)


def session_deduped(df) -> DataFrame:
    session_df = df.withColumn("_k", _try_cast("session_key", "int"))
    return dedup.dedup_latest(session_df, ["_k"])


# One row per (season, round, session_type) with its calendar date, from jolpica_schedule_raw.
def _schedule_sessions() -> DataFrame:
    snap = snapshot.latest_complete_jolpica(config.bronze("jolpica_schedule_raw"))
    p = F.from_json("payload", J.SCHEDULE)
    race = snap.select(F.explode(p["MRData"]["RaceTable"]["Races"]).alias("r"))
    races = (race.select(
        _try_cast("r.season", "int").alias("sched_season"),
        _try_cast("r.round", "int").alias("sched_round"),
        F.col("r.raceName").alias("sched_race_name"),
        F.col("r.Circuit.circuitId").alias("sched_circuit_id"),
        F.col("r.date").alias("_d_race"),
        F.col("r.Sprint.date").alias("_d_sprint"))
    )
    race_rows = (
        races
        .withColumn("sched_type", F.lit("Race"))
        .withColumn("sched_date", F.to_date("_d_race"))
    )
    sprint_rows = (
        races
        .where("_d_sprint IS NOT NULL")
        .withColumn("sched_type", F.lit("Sprint"))
        .withColumn("sched_date", F.to_date("_d_sprint"))
    )
    return race_rows.unionByName(sprint_rows).drop("_d_race", "_d_sprint")


def _meetings_clean() -> DataFrame:
    snap = snapshot.latest_valid_openf1(config.bronze("openf1_meetings_raw"))
    meeting = parse.explode_openf1(snap, O.MEETINGS)
    meeting_with_key = meeting.withColumn("_k", _try_cast("meeting_key", "int"))
    meeting = dedup.dedup_latest(meeting_with_key, ["_k"])
    return meeting.select(
        _try_cast("meeting_key", "int").alias("meeting_key"),
        F.trim("meeting_name").alias("meeting_name"),
        F.regexp_replace(F.trim("circuit_type"), " - ", " – ").alias("circuit_type")
    )


def _casted_sessions(df) -> DataFrame:
    return df.select(
        _try_cast("session_key", "int").alias("openf1_session_key"),
        _try_cast("meeting_key", "int").alias("meeting_key"),
        F.col("session_name").alias("session_type"),
        _try_cast("date_start", "timestamp").alias("session_start_utc"),
        _try_cast("date_end", "timestamp").alias("session_end_utc"),
        "gmt_offset",
        F.col("circuit_short_name").alias("circuit_name"),
        _try_cast("year", "int").alias("year"),
        F.col("_ingested_at").alias("ingested_at"),
        "_load_id"
    )


# OpenF1 has no round number: find the Jolpica calendar row by year, type and date (+-1 day)
def _matched_calendar(sessions, schedule) -> DataFrame:
    same_day = F.abs(F.datediff(F.to_date(sessions["session_start_utc"]), schedule["sched_date"])) <= 1
    matches = sessions.join(
        schedule,
        on=((sessions["year"] == schedule["sched_season"])
            & (sessions["session_type"] == schedule["sched_type"])
            & same_day),
        how="left"
    )
    session_w = Window.partitionBy("openf1_session_key")
    return (
        matches.withColumn("_n", F.count("sched_round").over(session_w))
            .withColumn("_rn", F.row_number().over(session_w.orderBy("sched_round")))
            .where("_rn = 1")
            .withColumn("season", F.when(F.col("_n") == 1, F.col("sched_season")))
            .withColumn("round", F.when(F.col("_n") == 1, F.col("sched_round")))
            .withColumn("_chk_schedule_matches", F.col("_n") == 1)
    )


def session_enriched(df) -> DataFrame:
    sessions = _matched_calendar(_casted_sessions(df), _schedule_sessions())
    joined = sessions.join(_meetings_clean(), "meeting_key", "left")
    local_start = F.expr("session_start_utc + CAST(gmt_offset AS INTERVAL HOUR TO SECOND)")
    return joined.select(
        session_id(F.col("season"), F.col("round"), F.col("session_type")).alias("session_id"),
        "openf1_session_key",
        "meeting_key",
        "season",
        "round",
        "session_type",
        F.coalesce("meeting_name", "sched_race_name").alias("event_name"),
        "circuit_name",
        "circuit_type",
        F.when(F.col("year") >= 2026, "2026 regulations")
            .otherwise("Pre-2026").alias("regulation_era"),
        "session_start_utc",
        "session_end_utc",
        "gmt_offset",
        F.to_date(local_start).alias("session_date_local"),
        F.col("sched_circuit_id").alias("jolpica_circuit_id"),
        "ingested_at",
        "_chk_schedule_matches",
        "_load_id",
    )

