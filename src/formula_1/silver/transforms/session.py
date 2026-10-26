from pyspark.sql import functions as F
from pyspark.sql import DataFrame, Window

from formula_1.silver import config
from formula_1.silver.schemas import jolpica as J, openf1 as O
from formula_1.silver.transforms import dedup, parse, snapshot, reference
from formula_1.silver.transforms.parse import _try_cast

SESSION_TYPES = ["Race", "Sprint"]


# session_id: 2026-04-R
def session_id(season, round, session_type):
    return F.concat(
        season.cast("string"), F.lit("-"),
        F.lpad(round.cast("string"), 2, "0"), F.lit("-"),
        F.when(session_type == "Race", "R").otherwise("S")
    )


# silver_session
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


# silver_session_driver
def session_driver_parsed() -> DataFrame:
    snap = snapshot.latest_valid_openf1(config.bronze("openf1_drivers_raw"))
    return parse.explode_openf1(snap, O.DRIVERS)


# Keep only drivers of sessions that exist in silver_session (Race and Sprint)
def session_driver_filtered(df, session) -> DataFrame:
    known = session.select(F.col("openf1_session_key").alias("_sk")).distinct()
    return df.join(
        known,
        on=_try_cast("session_key", "int") == known["_sk"],
        how="left_semi"
    )


def session_driver_deduped(df: DataFrame) -> DataFrame:
    keyed = (
        df.withColumn("_s", _try_cast("session_key", "int"))
        .withColumn("_n", _try_cast("driver_number", "int"))
    )
    return dedup.dedup_latest(keyed, ["_s", "_n"])


def _casted_drivers(df: DataFrame, session: DataFrame) -> DataFrame:
    drivers = df.select(
        _try_cast("session_key", "int").alias("openf1_session_key"),
        _try_cast("driver_number", "int").alias("driver_number"),
        F.upper(F.trim("name_acronym")).alias("name_acronym"),
        F.coalesce(F.col("full_name"), F.concat_ws(" ", "first_name", "last_name")).alias("full_name"),
        F.col("team_name").alias("team_name_openf1"),
        F.when(F.col("team_colour").rlike("^[0-9A-Fa-f]{6}$"), F.upper("team_colour")).alias("team_colour"),
        F.col("_ingested_at").alias("ingested_at"),
        "_load_id",
    )
    return drivers.join(
        session.select("openf1_session_key", "session_id"),
        on="openf1_session_key",
        how="left"
    )

# Jolpica results: (session_id, car number) -> driver and constructor, latest snapshot
def _car_numbers() -> DataFrame:

    def results_of(table, field, session_type) -> DataFrame:
        snap = snapshot.latest_complete_jolpica(config.bronze(table))
        return parse.explode_results(snap, field).select(
            session_id(_try_cast("Race.season", "int"),
                       _try_cast("Race.round", "int"),
                       F.lit(session_type)).alias("session_id"),
            _try_cast("Result.number", "int").alias("driver_number"),
            F.lower(F.trim("Result.Driver.driverId")).alias("number_driver_id"),
            F.lower(F.trim("Result.Constructor.constructorId")).alias("constructor_id"),
            "_load_id",
            "_ingested_at",
        )

    rows = results_of("jolpica_race_results_raw", "Results", "Race").unionByName(
        results_of("jolpica_sprint_results_raw", "SprintResults", "Sprint")
    )
    unique = dedup.dedup_latest(rows, ["session_id", "driver_number"])
    return unique.select("session_id", "driver_number", "number_driver_id", "constructor_id")


# driver_id -> Jolpica code (3 letters)
def _driver_codes() -> DataFrame:
    drivers = reference.driver_enriched(reference.driver_deduped(reference.driver_parsed()))
    return drivers.select("driver_id", F.col("name_acronym").alias("jolpica_code"))


# Codes that belong to exactly one driver: safe to match OpenF1 by acronym
def _unique_codes(codes: DataFrame) -> DataFrame:
    return (
        codes.where("jolpica_code IS NOT NULL")
        .groupBy(F.col("jolpica_code").alias("name_acronym"))
        .agg(F.count(F.lit(1)).alias("_n"), F.first("driver_id").alias("code_driver_id"))
        .where("_n = 1")
        .drop("_n")
    )


def session_driver_enriched(df: DataFrame, session: DataFrame) -> DataFrame:
    drivers = _casted_drivers(df, session)
    codes = _driver_codes()
    # method 1: car number in the Jolpica result; method 2: unique driver code
    joined = (
        drivers.join(_car_numbers(), ["session_id", "driver_number"], "left")
            .join(_unique_codes(codes), "name_acronym", "left")
            .withColumn("driver_id", F.coalesce("number_driver_id", "code_driver_id"))
            .join(codes, "driver_id", "left")
    )
    map_method = (
        F.when(F.col("number_driver_id").isNotNull(), "car_number")
        .when(F.col("code_driver_id").isNotNull(), "name_acronym")
        .otherwise("unmatched")
    )
    acronym_match = F.when(F.col("driver_id").isNotNull(), F.col("name_acronym") == F.col("jolpica_code"))
    return joined.select(
        "openf1_session_key",
        "driver_number",
        "session_id",
        "driver_id",
        "constructor_id",
        "name_acronym",
        "full_name",
        "team_name_openf1",
        "team_colour",
        map_method.alias("map_method"),
        acronym_match.alias("acronym_match"),
        "ingested_at",
        "_load_id",
    )

