from pyspark.sql import DataFrame, functions as F

from formula_1.gold.config import silver
from formula_1.gold.surrogate import date_sk, session_sk, sk
from formula_1.gold.transforms.dim_junk import LAP_FLAGS
from formula_1.gold.transforms.dim_team import lookup_team_sk


def fact_lap(dim_team: DataFrame) -> DataFrame:
    """One row per session, driver and lap from OpenF1."""
    sessions = silver("silver_session").select("openf1_session_key", "session_id", "session_date_local")
    laps = (
        silver("silver_lap")
        .where(F.col("driver_id").isNotNull())
        .join(sessions, "openf1_session_key", "left")
    )
    return lookup_team_sk(laps, dim_team, "session_date_local").select(
        session_sk("session_id").alias("session_sk"),
        sk("driver_id").alias("driver_sk"),
        "team_sk",
        date_sk("session_date_local").alias("session_date_sk"),
        sk("track_status_name").alias("track_status_sk"),
        sk(*LAP_FLAGS).alias("lap_flags_sk"),
        sk("compound").alias("tyre_compound_sk"),
        "lap_duration_ms",
        "tyre_age_laps",
        "position_at_lap_end",
        "lap_number",
        "stint_number",
    )