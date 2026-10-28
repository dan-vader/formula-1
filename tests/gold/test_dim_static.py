import pytest
from pyspark.sql import functions as F

from formula_1.gold.surrogate import UNKNOWN_SK, sk
from formula_1.gold.transforms.dim_static import TYRE_COMPOUNDS, dim_track_status, dim_tyre_compound
from formula_1.silver.transforms.laps import COMPOUNDS

ALL_TYRES = {"SOFT", "MEDIUM", "HARD", "INTERMEDIATE", "WET"}


def known_rows(dim, sk_col, name_col):
    rows = dim.where(F.col(sk_col) != UNKNOWN_SK).collect()
    return {row[name_col]: row.asDict() for row in rows}


# шини

def test_tyre_compound_has_five_tyres_plus_unknown(spark):
    dim = dim_tyre_compound()

    assert set(known_rows(dim, "tyre_compound_sk", "compound_name")) == ALL_TYRES
    assert dim.count() == 6


def test_only_dry_tyres_are_slick_and_ranked_by_hardness(spark):
    rows = known_rows(dim_tyre_compound(), "tyre_compound_sk", "compound_name")

    slicks = {name: row["hardness_rank"] for name, row in rows.items() if row["is_slick"]}

    assert slicks == {"SOFT": 1, "MEDIUM": 2, "HARD": 3}
    assert rows["INTERMEDIATE"]["hardness_rank"] is None
    assert rows["WET"]["hardness_rank"] is None


@pytest.mark.parametrize("compound", sorted(ALL_TYRES))
def test_fact_compound_finds_its_tyre_row(spark, compound):
    facts = spark.createDataFrame([(compound,)], "compound string")
    fact_keys = facts.select(sk("compound").alias("tyre_compound_sk"))

    missing = fact_keys.join(dim_tyre_compound(), "tyre_compound_sk", "left_anti")

    assert missing.count() == 0


def test_gold_knows_every_tyre_that_silver_lets_through():
    assert {name for name, _, _ in TYRE_COMPOUNDS} == set(COMPOUNDS)


# стан траси

def test_track_status_has_four_statuses_plus_unknown(spark):
    dim = dim_track_status()

    names = set(known_rows(dim, "track_status_sk", "track_status_name"))

    assert names == {"Green", "Virtual Safety Car", "Safety Car", "Red Flag"}
    assert dim.count() == 5


def test_only_green_is_not_neutralised(spark):
    rows = known_rows(dim_track_status(), "track_status_sk", "track_status_name")

    not_neutralised = {name for name, row in rows.items() if not row["is_neutralised"]}

    assert not_neutralised == {"Green"}


@pytest.mark.parametrize(
    ("build", "sk_col"),
    [
        pytest.param(dim_tyre_compound, "tyre_compound_sk", id="tyre_compound"),
        pytest.param(dim_track_status, "track_status_sk", id="track_status"),
    ],
)
def test_static_dim_keys_are_unique(spark, build, sk_col):
    dim = build()

    assert dim.select(sk_col).distinct().count() == dim.count()