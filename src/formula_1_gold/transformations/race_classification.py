from pyspark import pipelines as dp
from pyspark.sql import functions as F

from formula_1.gold import factory
from formula_1.gold.config import silver
from formula_1.gold.schemas.gold import FACT_RACE_CLASSIFICATION_DDL
from formula_1.gold.transforms.fact_race_classification import fact_race_classification

factory.register_fact(
    "fact_transaction_race_classification",
    lambda: fact_race_classification(dp.read("dim_team")),
    FACT_RACE_CLASSIFICATION_DDL,
    comment="Race and Sprint classification, one row per published version",
    grain=["session_sk", "driver_sk", "classification_version"],
    references={
        "session_sk": "dim_session.session_sk",
        "driver_sk": "dim_driver.driver_sk",
        "session_date_sk": "dim_date.date_sk",
        "classification_date_sk": "dim_date.date_sk",
        "finish_status_sk": "dim_finish_status.finish_status_sk",
        "team_sk": "dim_team.team_sk",
    },
    warnings={
        "grid_position_valid": "grid_position IS NULL OR grid_position >= 1",
        "classified_position_valid": "classified_position IS NULL OR classified_position >= 1",
        "points_not_negative": "points >= 0",
    },
)


@dp.materialized_view(name="fact_transaction_race_classification_reconciliation", private=True)
@dp.expect_or_fail("no_versions_lost", "fact_rows = silver_rows")
@dp.expect_or_fail("points_change_adds_up", "fact_points_change = silver_latest_points")
def _race_classification_reconciliation():
    results = silver("silver_race_result")
    latest_points = results.groupBy("session_id", "driver_id").agg(
        F.max_by("points", "snapshot_ts").alias("points")
    )
    source = results.agg(F.count(F.lit(1)).alias("silver_rows")).crossJoin(
        latest_points.agg(F.sum("points").alias("silver_latest_points"))
    )
    target = dp.read("fact_transaction_race_classification").agg(
        F.count(F.lit(1)).alias("fact_rows"),
        F.sum("points_change").alias("fact_points_change"),
    )
    return source.crossJoin(target)