from datetime import date

import pytest
from pyspark.sql import functions as F

from formula_1.gold import config as gold_config
from formula_1.gold.surrogate import UNKNOWN_SK, date_sk
from formula_1.gold.transforms import dim_date as dim_date_module
from formula_1.gold.transforms.dim_date import dim_date


@pytest.fixture
def two_seasons(monkeypatch):
    monkeypatch.setattr(dim_date_module, "seasons", lambda: [2024, 2025])


def by_date(dim):
    rows = dim.where(F.col("date_sk") != UNKNOWN_SK).collect()
    return {row["full_date"]: row.asDict() for row in rows}


def test_dim_date_covers_every_day_of_seasons_plus_unknown(spark, two_seasons):
    dim = dim_date()

    days = by_date(dim)

    assert min(days) == date(2024, 1, 1)
    assert max(days) == date(2025, 12, 31)
    assert len(days) == 366 + 365
    assert dim.count() == len(days) + 1


def test_dim_date_includes_leap_day(spark, two_seasons):
    assert date(2024, 2, 29) in by_date(dim_date())


def test_dim_date_row_describes_the_day(spark, two_seasons):
    days = by_date(dim_date())

    assert days[date(2025, 12, 7)] == {
        "date_sk": 20251207,
        "full_date": date(2025, 12, 7),
        "day_of_week": "Sunday",
        "month_number": 12,
        "month_name": "December",
        "quarter": "Q4",
        "year": 2025,
    }


def test_dim_date_keys_and_dates_are_unique(spark, two_seasons):
    dim = dim_date()

    assert dim.select("date_sk").distinct().count() == dim.count()
    assert dim.select("full_date").distinct().count() == dim.count()


def test_fact_date_finds_its_row_in_dim_date(spark, two_seasons):
    facts = spark.createDataFrame([(date(2024, 3, 2),), (date(2025, 12, 7),)], "session_date date")
    fact_keys = facts.select(date_sk("session_date").alias("date_sk"))

    missing = fact_keys.join(dim_date(), "date_sk", "left_anti")

    assert missing.count() == 0


def test_dim_date_with_one_season(spark, monkeypatch):
    monkeypatch.setattr(dim_date_module, "seasons", lambda: [2025])

    assert dim_date().count() == 365 + 1


def test_seasons_are_read_from_spark_config_and_sorted(spark):
    spark.conf.set("formula_1.seasons", "2025,2024")
    try:
        assert gold_config.seasons() == [2024, 2025]
    finally:
        spark.conf.unset("formula_1.seasons")