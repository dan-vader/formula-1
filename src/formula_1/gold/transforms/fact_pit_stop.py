from pyspark.sql import DataFrame, functions as F

from formula_1.gold.config import silver
from formula_1.gold.surrogate import date_sk, session_sk, sk
from formula_1.gold.transforms.dim_junk import PIT_STOP_FLAGS
from formula_1.gold.transforms.dim_team import lookup_team_sk


def fact_pit_stop(dim_team: DataFrame) -> DataFrame:
    sessions = silver("silver_session").select("openf1_session_key", "session_id", "session_date_local")
    pits = (
        silver("silver_pit_stop")
        .where(F.col("driver_id").isNotNull())
        .join(sessions, "openf1_session_key", "left")
    )
    return lookup_team_sk(pits, dim_team, "session_date_local").select(
        session_sk("session_id").alias("session_sk"),
        sk("driver_id").alias("driver_sk"),
        "team_sk",
        date_sk("session_date_local").alias("session_date_sk"),
        sk("compound_removed").alias("tyre_compound_removed_sk"),
        sk("compound_fitted").alias("tyre_compound_fitted_sk"),
        sk("track_status_name").alias("track_status_sk"),
        sk(*PIT_STOP_FLAGS).alias("pit_stop_flags_sk"),
        "pit_lane_duration_ms",
        "stop_duration_ms",
        "positions_lost",
        "lap_number",
    )