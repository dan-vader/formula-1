from pyspark import pipelines as dp

from pyspark.sql import functions as F

from formula_1.gold import factory
from formula_1.gold.schemas.gold import FACT_LAP_DDL
from formula_1.gold.transforms.fact_lap import fact_lap
from formula_1.gold.config import silver

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



@dp.materialized_view(name="fact_transaction_lap_reconciliation", private=True)
@dp.expect_or_fail("no_laps_lost", "fact_laps = silver_laps_with_driver")
@dp.expect("all_laps_have_driver", "silver_laps_without_driver = 0")
def _lap_reconciliation():
    source = silver("silver_lap").agg(
        F.count_if(F.col("driver_id").isNotNull()).alias("silver_laps_with_driver"),
        F.count_if(F.col("driver_id").isNull()).alias("silver_laps_without_driver"),
    )
    target = dp.read("fact_transaction_lap").agg(F.count(F.lit(1)).alias("fact_laps"))
    return source.crossJoin(target)