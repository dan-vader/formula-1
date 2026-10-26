from pyspark.sql import DataFrame, functions as F

from formula_1.gold.surrogate import sk, with_unknown_member
from formula_1.silver.config import _spark

TYRE_COMPOUNDS = [
    ("SOFT", True, 1),
    ("MEDIUM", True, 2),
    ("HARD", True, 3),
    ("INTERMEDIATE", False, None),
    ("WET", False, None),
]

TRACK_STATUSES = [
    ("Green", False),
    ("Virtual Safety Car", True),
    ("Safety Car", True),
    ("Red Flag", True),
]


def dim_tyre_compound() -> DataFrame:
    rows = _spark().createDataFrame(
        TYRE_COMPOUNDS, "compound_name string, is_slick boolean, hardness_rank int"
    )
    dim = rows.select(
        sk("compound_name").alias("tyre_compound_sk"),
        "compound_name",
        "is_slick",
        "hardness_rank",
        F.current_timestamp().alias("loaded_at"),
        F.current_timestamp().alias("updated_at"),
    )
    return with_unknown_member(dim, "tyre_compound_sk")


def dim_track_status() -> DataFrame:
    rows = _spark().createDataFrame(TRACK_STATUSES, "track_status_name string, is_neutralised boolean")
    dim = rows.select(
        sk("track_status_name").alias("track_status_sk"),
        "track_status_name",
        "is_neutralised",
        F.current_timestamp().alias("loaded_at"),
        F.current_timestamp().alias("updated_at"),
    )
    return with_unknown_member(dim, "track_status_sk")