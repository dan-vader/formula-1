from pyspark import pipelines as dp

from formula_1.gold import factory
from formula_1.gold.schemas.gold import FACT_LAP_DDL
from formula_1.gold.transforms.fact_lap import fact_lap

factory.register_fact(
    "fact_transaction_lap",
    lambda: fact_lap(dp.read("dim_team")),
    FACT_LAP_DDL,
    comment="One row per lap of a driver in a Race or Sprint",
    grain=["session_sk", "driver_sk", "lap_number"],
    references={
        "session_sk": "dim_session.session_sk",
        "driver_sk": "dim_driver.driver_sk",
        "team_sk": "dim_team.team_sk",
        "session_date_sk": "dim_date.date_sk",
        "track_status_sk": "dim_track_status.track_status_sk",
        "lap_flags_sk": "dim_lap_flags.lap_flags_sk",
        "tyre_compound_sk": "dim_tyre_compound.tyre_compound_sk",
    },
    warnings={
        "lap_duration_positive": "lap_duration_ms IS NULL OR lap_duration_ms > 0",
        "tyre_age_not_negative": "tyre_age_laps IS NULL OR tyre_age_laps >= 0",
        "position_valid": "position_at_lap_end IS NULL OR position_at_lap_end >= 1",
    },
)

factory.register_reconciliation("fact_transaction_lap", "silver_lap", kept="driver_id IS NOT NULL")