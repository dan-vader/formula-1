from pyspark import pipelines as dp

from formula_1.gold import factory
from formula_1.gold.schemas.gold import FACT_PIT_STOP_DDL
from formula_1.gold.transforms.fact_pit_stop import fact_pit_stop

factory.register_fact(
    "fact_transaction_pit_stop",
    lambda: fact_pit_stop(dp.read("dim_team")),
    FACT_PIT_STOP_DDL,
    comment="One row per pit lane visit of a driver in a Race or Sprint",
    grain=["session_sk", "driver_sk", "lap_number"],
    references={
        "session_sk": "dim_session.session_sk",
        "driver_sk": "dim_driver.driver_sk",
        "team_sk": "dim_team.team_sk",
        "session_date_sk": "dim_date.date_sk",
        "tyre_compound_removed_sk": "dim_tyre_compound.tyre_compound_sk",
        "tyre_compound_fitted_sk": "dim_tyre_compound.tyre_compound_sk",
        "track_status_sk": "dim_track_status.track_status_sk",
        "pit_stop_flags_sk": "dim_pit_stop_flags.pit_stop_flags_sk",
    },
    warnings={
        "pit_lane_duration_positive": "pit_lane_duration_ms > 0",
        "stop_within_pit_lane": "stop_duration_ms IS NULL OR stop_duration_ms <= pit_lane_duration_ms",
    },
)

factory.register_reconciliation("fact_transaction_pit_stop", "silver_pit_stop", kept="driver_id IS NOT NULL")