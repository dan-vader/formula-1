from datetime import date, timedelta
from itertools import pairwise

import pytest
from pyspark.sql import functions as F

from formula_1.gold.surrogate import UNKNOWN_SK
from formula_1.gold.transforms import dim_team as dim_team_module
from formula_1.gold.transforms.dim_team import dim_team, lookup_team_sk

TEAM_SCHEMA = (
    "constructor_id string, season int, power_unit_supplier string, "
    "team_name string, team_lineage string, team_colour string"
)
FACTS_SCHEMA = "constructor_id string, race_date date"
OPEN_END = date(9999, 12, 31)

SEASONS = [
    ("mclaren", 2020, "Renault", "McLaren", "McLaren", "FF8000"),
    ("mclaren", 2021, "Mercedes", "McLaren", "McLaren", "FF8000"),
    ("williams", 2020, "Mercedes", "Williams", "Williams", "005AFF"),
    ("williams", 2021, "Mercedes", "Williams", "Williams", "005AFF"),
    ("racing_point", 2020, "Mercedes", "Racing Point", "Aston Martin", "F596C8"),
    ("aston_martin", 2021, "Mercedes", "Aston Martin", "Aston Martin", "006F62"),
]


def use_silver_team(spark, monkeypatch, rows):
    df = spark.createDataFrame(rows, TEAM_SCHEMA)

    def fake_silver(table):
        assert table == "silver_team"
        return df

    monkeypatch.setattr(dim_team_module, "silver", fake_silver)


def versions(dim):
    rows = dim.where(F.col("team_sk") != UNKNOWN_SK).collect()
    return {(row["constructor_id"], row["effective_from"]): row.asDict() for row in rows}


def versions_of(dim, constructor_id):
    rows = [row for (team, _), row in versions(dim).items() if team == constructor_id]
    return sorted(rows, key=lambda row: row["effective_from"])


@pytest.fixture
def teams(spark, monkeypatch):
    use_silver_team(spark, monkeypatch, SEASONS)
    return dim_team()


def test_team_gets_new_version_when_engine_supplier_changes(teams):
    old, new = versions_of(teams, "mclaren")

    assert (old["power_unit_supplier"], old["effective_from"], old["effective_to"]) == (
        "Renault", date(2020, 1, 1), date(2020, 12, 31)
    )
    assert (new["power_unit_supplier"], new["effective_from"], new["effective_to"]) == (
        "Mercedes", date(2021, 1, 1), OPEN_END
    )


def test_same_supplier_in_consecutive_seasons_gives_one_version(teams):
    rows = versions_of(teams, "williams")

    assert len(rows) == 1
    assert (rows[0]["effective_from"], rows[0]["effective_to"]) == (date(2020, 1, 1), OPEN_END)


def test_retired_team_ends_on_last_day_of_its_last_season(teams):
    (row,) = versions_of(teams, "racing_point")

    assert row["effective_to"] == date(2020, 12, 31)
    assert row["is_current"] is False


def test_only_latest_version_of_active_team_is_current(teams):
    current = {key for key, row in versions(teams).items() if row["is_current"]}

    assert current == {
        ("mclaren", date(2021, 1, 1)),
        ("williams", date(2020, 1, 1)),
        ("aston_martin", date(2021, 1, 1)),
    }


def test_versions_of_team_have_no_gaps_or_overlaps(teams):
    for team in ["mclaren", "williams", "racing_point", "aston_martin"]:
        rows = versions_of(teams, team)
        for older, newer in pairwise(rows):
            assert older["effective_to"] + timedelta(days=1) == newer["effective_from"], team


def test_returning_to_old_supplier_starts_new_version(spark, monkeypatch):
    use_silver_team(spark, monkeypatch, [
        ("lotus", 2019, "Renault", "Lotus", "Lotus", "FFB800"),
        ("lotus", 2020, "Mercedes", "Lotus", "Lotus", "FFB800"),
        ("lotus", 2021, "Renault", "Lotus", "Lotus", "FFB800"),
    ])

    suppliers = [row["power_unit_supplier"] for row in versions_of(dim_team(), "lotus")]

    assert suppliers == ["Renault", "Mercedes", "Renault"]


def test_all_versions_get_latest_name_and_colour(spark, monkeypatch):
    use_silver_team(spark, monkeypatch, [
        ("sauber", 2020, "Ferrari", "Alfa Romeo", "Sauber", "900000"),
        ("sauber", 2021, "Audi", "Audi", "Sauber", "BB0A30"),
    ])

    rows = versions_of(dim_team(), "sauber")

    assert len(rows) == 2
    assert {(row["team_name"], row["team_colour"]) for row in rows} == {("Audi", "BB0A30")}


def test_team_keys_are_unique(teams):
    assert teams.select("team_sk").distinct().count() == teams.count()


def test_unknown_team_is_not_current(teams):
    row = teams.where(F.col("team_sk") == UNKNOWN_SK).first()

    assert row["constructor_id"] == "Unknown"
    assert row["is_current"] is False


def sk_of(dim, constructor_id, effective_from):
    return versions(dim)[(constructor_id, effective_from)]["team_sk"]


def test_lookup_picks_version_valid_on_race_date(spark, teams):
    facts = spark.createDataFrame(
        [("mclaren", date(2020, 7, 5)), ("mclaren", date(2021, 3, 28))], FACTS_SCHEMA
    )

    result = {row["race_date"]: row["team_sk"] for row in lookup_team_sk(facts, teams, "race_date").collect()}

    assert result == {
        date(2020, 7, 5): sk_of(teams, "mclaren", date(2020, 1, 1)),
        date(2021, 3, 28): sk_of(teams, "mclaren", date(2021, 1, 1)),
    }


@pytest.mark.parametrize(
    ("constructor_id", "race_date"),
    [
        pytest.param("brawn", date(2020, 5, 1), id="unknown_team"),
        pytest.param(None, date(2020, 5, 1), id="no_team"),
        pytest.param("racing_point", date(2021, 5, 1), id="after_team_retired"),
        pytest.param("mclaren", None, id="no_date"),
    ],
)
def test_lookup_gives_unknown_when_no_version_matches(spark, teams, constructor_id, race_date):
    facts = spark.createDataFrame([(constructor_id, race_date)], FACTS_SCHEMA)

    result = lookup_team_sk(facts, teams, "race_date").first()

    assert result["team_sk"] == UNKNOWN_SK


def test_lookup_keeps_every_fact_row_and_only_adds_team_sk(spark, teams):
    facts = spark.createDataFrame(
        [
            ("mclaren", date(2020, 7, 5)),
            ("williams", date(2021, 3, 28)),
            ("brawn", date(2020, 5, 1)),
        ],
        FACTS_SCHEMA,
    )

    result = lookup_team_sk(facts, teams, "race_date")

    assert result.count() == facts.count()
    assert result.columns == facts.columns + ["team_sk"]    