from datetime import datetime

import pytest
from pyspark.sql import functions as F

from formula_1.silver.transforms.reference import (
    driver_deduped,
    driver_enriched,
    finish_status_deduped,
    finish_status_enriched,
    status_category,
)

STATUS_SCHEMA = "status struct<statusId: string, status: string>, _load_id string, _ingested_at timestamp"
DRIVER_SCHEMA = (
    "d struct<driverId: string, permanentNumber: string, code: string, givenName: string, familyName: string>, "
    "_season string, _load_id string, _ingested_at timestamp"
)
L1 = "20251201T090000Z"
L2 = "20251208T090000Z"
INGESTED = datetime(2025, 12, 8, 9, 0)


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        ("Finished", "Classified"),
        ("Lapped", "Classified"),
        ("+1 Lap", "Classified"),
        ("+3 Laps", "Classified"),
        ("  Finished  ", "Classified"),
        ("Disqualified", "Disqualified"),
        ("Excluded", "Disqualified"),
        ("Did not start", "Did not start"),
        ("Engine", "Retired"),
        ("Accident", "Retired"),
    ],
)
def test_status_category(spark, status, expected):
    df = spark.createDataFrame([(status,)], "status string")

    result = df.select(status_category(F.col("status")).alias("category")).first()

    assert result["category"] == expected


def test_finish_status_deduped_keeps_newest_load_per_status_id(spark):
    df = spark.createDataFrame(
        [(("1", "Finished"), L1, INGESTED), (("1", "Finished "), L2, INGESTED), (("2", "Engine"), L1, INGESTED)],
        STATUS_SCHEMA,
    )

    result = finish_status_deduped(df)

    rows = {row["status"]["statusId"]: row["_load_id"] for row in result.collect()}
    assert rows == {"1": L2, "2": L1}


def test_finish_status_enriched_trims_and_adds_category(spark):
    df = spark.createDataFrame([(("11", "  +1 Lap "), L2, INGESTED)], STATUS_SCHEMA)

    result = finish_status_enriched(df).first().asDict()

    assert result == {
        "status_id": 11,
        "status": "+1 Lap",
        "status_category": "Classified",
        "ingested_at": INGESTED,
        "_load_id": L2,
    }


def test_finish_status_enriched_sets_null_id_when_it_is_not_a_number(spark):
    df = spark.createDataFrame([(("abc", "Finished"), L2, INGESTED)], STATUS_SCHEMA)

    result = finish_status_enriched(df).first()

    assert result["status_id"] is None


def driver(driver_id, given="Lewis", family="Hamilton", code="HAM", number="44"):
    return (driver_id, number, code, given, family)


def test_driver_deduped_merges_ids_ignoring_case_and_spaces(spark):
    df = spark.createDataFrame(
        [(driver("hamilton"), "2025", L1, INGESTED), (driver(" Hamilton "), "2025", L2, INGESTED)],
        DRIVER_SCHEMA,
    )

    result = driver_deduped(df)

    assert [row["_load_id"] for row in result.collect()] == [L2]


def test_driver_deduped_prefers_newest_season(spark):
    df = spark.createDataFrame(
        [(driver("hamilton", number="44"), "2025", L1, INGESTED), (driver("hamilton", number="0"), "2024", L2, INGESTED)],
        DRIVER_SCHEMA,
    )

    result = driver_deduped(df)

    assert [row["_season"] for row in result.collect()] == ["2025"]


def test_driver_enriched_builds_names_code_and_number(spark):
    df = spark.createDataFrame(
        [(driver(" Hamilton ", given=" Lewis ", family=" Hamilton ", code="ham", number="44"), "2025", L2, INGESTED)],
        DRIVER_SCHEMA,
    )

    result = driver_enriched(df).first().asDict()

    assert result == {
        "driver_id": "hamilton",
        "given_name": "Lewis",
        "family_name": "Hamilton",
        "full_name": "Lewis Hamilton",
        "name_acronym": "HAM",
        "permanent_number": 44,
        "ingested_at": INGESTED,
        "_load_id": L2,
    }


def test_driver_enriched_handles_missing_number_and_given_name(spark):
    df = spark.createDataFrame(
        [(driver("fangio", given=None, family="Fangio", code=None, number=None), "1955", L2, INGESTED)],
        DRIVER_SCHEMA,
    )

    result = driver_enriched(df).first()

    assert result["full_name"] == "Fangio"
    assert result["name_acronym"] is None
    assert result["permanent_number"] is None

