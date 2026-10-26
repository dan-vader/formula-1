from pyspark.sql import DataFrame, functions as F

from formula_1.gold.config import seasons
from formula_1.gold.surrogate import date_sk, with_unknown_member
from formula_1.silver.config import _spark


def dim_date() -> DataFrame:
    """One row per calendar day of every season in scope, plus the unknown member."""
    years = seasons()
    first = F.to_date(F.lit(f"{years[0]}-01-01"))
    last = F.to_date(F.lit(f"{years[-1]}-12-31"))
    days = _spark().range(1).select(F.explode(F.sequence(first, last)).alias("full_date"))
    dim = days.select(
        date_sk("full_date").alias("date_sk"),
        "full_date",
        F.date_format("full_date", "EEEE").alias("day_of_week"),
        F.month("full_date").alias("month_number"),
        F.date_format("full_date", "MMMM").alias("month_name"),
        F.concat(F.lit("Q"), F.quarter("full_date")).alias("quarter"),
        F.year("full_date").alias("year"),
        F.current_timestamp().alias("loaded_at"),
    )
    return with_unknown_member(dim, "date_sk")