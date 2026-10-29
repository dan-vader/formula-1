from datetime import date, datetime

import pytest
from pyspark.sql import functions as F

from formula_1.gold.surrogate import (
    UNKNOWN_SK,
    date_sk,
    session_sk,
    sk,
    with_inferred_members,
    with_unknown_member,
)


def keys(spark, rows, schema, key):
    df = spark.createDataFrame(rows, schema)
    return [row["k"] for row in df.select(key.alias("k")).collect()]


def test_sk_is_same_for_same_value(spark):
    result = keys(spark, [("hamilton",), ("hamilton",)], "driver_id string", sk("driver_id"))

    assert result[0] == result[1]
    assert result[0] != UNKNOWN_SK


def test_sk_differs_for_different_values(spark):
    result = keys(spark, [("hamilton",), ("leclerc",)], "driver_id string", sk("driver_id"))

    assert result[0] != result[1]


def test_sk_is_unknown_when_key_is_null(spark):
    result = keys(spark, [(None,)], "driver_id string", sk("driver_id"))

    assert result == [UNKNOWN_SK]


@pytest.mark.parametrize(
    ("constructor_id", "effective_from"),
    [("ferrari", None), (None, date(2025, 1, 1))],
)
def test_sk_is_unknown_when_any_part_of_composite_key_is_null(spark, constructor_id, effective_from):
    result = keys(
        spark,
        [(constructor_id, effective_from)],
        "constructor_id string, effective_from date",
        sk("constructor_id", "effective_from"),
    )

    assert result == [UNKNOWN_SK]


def test_sk_of_composite_key_depends_on_every_part(spark):
    result = keys(
        spark,
        [("ferrari", date(2014, 1, 1)), ("ferrari", date(2026, 1, 1))],
        "constructor_id string, effective_from date",
        sk("constructor_id", "effective_from"),
    )

    assert result[0] != result[1]


# date_sk

@pytest.mark.parametrize(
    ("value", "expected"),
    [(date(2025, 12, 7), 20251207), (date(2026, 1, 1), 20260101)],
)
def test_date_sk_is_yyyymmdd(spark, value, expected):
    result = keys(spark, [(value,)], "d date", date_sk("d"))

    assert result == [expected]


def test_date_sk_takes_day_from_timestamp(spark):
    result = keys(spark, [(datetime(2025, 12, 7, 23, 30),)], "ts timestamp", date_sk("ts"))

    assert result == [20251207]


def test_date_sk_is_unknown_when_date_is_null(spark):
    result = keys(spark, [(None,)], "d date", date_sk("d"))

    assert result == [UNKNOWN_SK]


# session_sk

@pytest.mark.parametrize(
    ("session_id", "expected"),
    [
        ("2025-24-R", 2025242),
        ("2025-04-S", 2025041),
        ("2026-01-R", 2026012),
    ],
)
def test_session_sk_encodes_season_round_and_type(spark, session_id, expected):
    result = keys(spark, [(session_id,)], "session_id string", session_sk("session_id"))

    assert result == [expected]


@pytest.mark.parametrize(
    "session_id",
    ["2025-4-R", "2025-24-Q", "2025_24_R", "2025-24-RX", "", None],
)
def test_session_sk_is_unknown_for_invalid_session_id(spark, session_id):
    result = keys(spark, [(session_id,)], "session_id string", session_sk("session_id"))

    assert result == [UNKNOWN_SK]


def test_session_sk_orders_sessions_chronologically(spark):
    rows = [("2025-23-R",), ("2025-24-S",), ("2025-24-R",)]

    result = keys(spark, rows, "session_id string", session_sk("session_id"))

    assert result == sorted(result)


# with_unknown_member

TEAM_SCHEMA = "team_sk bigint, team_name string, hardness int, is_current boolean"


def team_dim(spark):
    return spark.createDataFrame(
        [(1, "Ferrari", 3, True), (2, "McLaren", 2, True)],
        TEAM_SCHEMA,
    )


def by_key(df, key):
    return {row[key]: row.asDict() for row in df.collect()}


def test_unknown_member_adds_exactly_one_row(spark):
    result = with_unknown_member(team_dim(spark), "team_sk")

    assert result.count() == 3


def test_unknown_member_has_minus_one_key_and_unknown_text(spark):
    result = by_key(with_unknown_member(team_dim(spark), "team_sk"), "team_sk")

    assert result[UNKNOWN_SK] == {
        "team_sk": UNKNOWN_SK,
        "team_name": "Unknown",
        "hardness": None,
        "is_current": None,
    }


def test_unknown_member_uses_given_override(spark):
    result = by_key(with_unknown_member(team_dim(spark), "team_sk", is_current=False), "team_sk")

    assert result[UNKNOWN_SK]["is_current"] is False


def test_unknown_member_keeps_existing_rows_and_types(spark):
    dim = team_dim(spark)

    result = with_unknown_member(dim, "team_sk")

    rows = by_key(result, "team_sk")
    assert rows[1] == {"team_sk": 1, "team_name": "Ferrari", "hardness": 3, "is_current": True}
    assert result.dtypes == dim.dtypes


# with_inferred_members

KNOWN_SCHEMA = "driver_id string, full_name string, is_inferred boolean, inferred_at timestamp"
REFERENCED_SCHEMA = "driver_id string, inferred_at timestamp"
T1 = datetime(2025, 12, 7, 15, 0)
T2 = datetime(2025, 12, 8, 9, 0)


def known_drivers(spark):
    return spark.createDataFrame([("hamilton", "Lewis Hamilton", False, None)], KNOWN_SCHEMA)


def test_inferred_members_adds_missing_driver_as_inferred(spark):
    referenced = spark.createDataFrame([("colapinto", T2)], REFERENCED_SCHEMA)

    result = by_key(with_inferred_members(known_drivers(spark), referenced, "driver_id"), "driver_id")

    assert result["colapinto"] == {
        "driver_id": "colapinto",
        "full_name": "Unknown",
        "is_inferred": True,
        "inferred_at": T2,
    }


def test_inferred_members_does_not_duplicate_known_driver(spark):
    referenced = spark.createDataFrame([("hamilton", T1)], REFERENCED_SCHEMA)

    result = with_inferred_members(known_drivers(spark), referenced, "driver_id")

    rows = result.collect()
    assert len(rows) == 1
    assert rows[0]["is_inferred"] is False


def test_inferred_members_ignores_null_key(spark):
    referenced = spark.createDataFrame([(None, T1)], REFERENCED_SCHEMA)

    result = with_inferred_members(known_drivers(spark), referenced, "driver_id")

    assert result.count() == 1


def test_inferred_members_uses_given_override(spark):
    referenced = spark.createDataFrame([("colapinto", T2)], REFERENCED_SCHEMA)

    result = with_inferred_members(
        known_drivers(spark), referenced, "driver_id", full_name=F.upper("driver_id")
    )

    assert by_key(result, "driver_id")["colapinto"]["full_name"] == "COLAPINTO"