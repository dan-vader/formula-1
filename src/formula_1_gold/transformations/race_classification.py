from pyspark import pipelines as dp

from formula_1.gold import factory
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