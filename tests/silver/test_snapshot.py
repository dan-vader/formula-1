import json
from datetime import datetime

from formula_1.silver.transforms.snapshot import (
    all_complete_jolpica,
    complete_jolpica_loads,
    csv_latest,
    latest_complete_jolpica,
    latest_valid_openf1,
)

SUPPLIER_SCHEMA = "constructor_id string, season int, power_unit_supplier string, _ingested_at timestamp"
OLD = datetime(2025, 12, 1, 9, 0)
NEW = datetime(2025, 12, 8, 9, 0)
PAGE_SCHEMA = "_season string, _load_id string, _page_offset int, payload string"
LOAD_ID = "20251208T090000Z"
JOLPICA_SCHEMA = "_season string, _load_id string, value string"
OPENF1_SCHEMA = "_year int, _load_id string, value string"
MARKER_SCHEMA = "_load_id string, payload string"

L1 = "20251201T090000Z"
L2 = "20251208T090000Z"
L3 = "20251209T090000Z"


def marker(load_id, **fields):
    return (load_id, json.dumps({"load_id": load_id, **fields}))


def season_load_pairs(result):
    return sorted((row["_season"], row["_load_id"]) for row in result.collect())


def page(season, offset, limit, total, load_id=LOAD_ID):
    payload = json.dumps({"MRData": {"limit": str(limit), "total": str(total)}})
    return (season, load_id, offset, payload)


def test_csv_latest_keeps_newest_ingested_row(spark):
    df = spark.createDataFrame(
        [
            ("williams", 2025, "Renault", OLD),
            ("williams", 2025, "Mercedes", NEW),
        ],
        SUPPLIER_SCHEMA,
    )

    result = csv_latest(df, ["constructor_id", "season"])

    assert [row["power_unit_supplier"] for row in result.collect()] == ["Mercedes"]

def test_csv_latest_keeps_one_row_per_constructor_and_season(spark):
    df = spark.createDataFrame(
        [
            ("ferrari", 2024, "Ferrari", OLD),
            ("ferrari", 2025, "Ferrari", NEW),
            ("sauber", 2025, "Ferrari", OLD),
        ],
        SUPPLIER_SCHEMA,
    )

    result = csv_latest(df, ["constructor_id", "season"])

    keys = sorted((row["constructor_id"], row["season"]) for row in result.collect())
    assert keys == [("ferrari", 2024), ("ferrari", 2025), ("sauber", 2025)]


def test_csv_latest_keeps_columns_and_drops_helper(spark):
    df = spark.createDataFrame([("williams", 2025, "Mercedes", NEW)], SUPPLIER_SCHEMA)

    result = csv_latest(df, ["constructor_id", "season"])

    assert result.columns == df.columns


def test_csv_latest_keeps_key_that_is_missing_in_newer_file(spark):
    df = spark.createDataFrame(
        [
            ("williams", 2025, "Mercedes", OLD),
            ("alpine", 2025, "Renault", OLD),
            ("williams", 2025, "Mercedes", NEW),
        ],
        SUPPLIER_SCHEMA,
    )

    result = csv_latest(df, ["constructor_id", "season"])

    # Current behaviour: a key removed from the CSV stays in silver with its old value
    assert {row["constructor_id"] for row in result.collect()} == {"williams", "alpine"}

def test_complete_jolpica_loads_returns_load_with_all_pages(spark):
    df = spark.createDataFrame(
        [page("2025", 0, 30, 70), page("2025", 30, 30, 70), page("2025", 60, 30, 70)],
        PAGE_SCHEMA,
    )

    result = complete_jolpica_loads(df, ["_season"])

    assert [(row["_season"], row["_load_id"]) for row in result.collect()] == [("2025", LOAD_ID)]

def test_complete_jolpica_loads_skips_load_with_missing_page(spark):
    df = spark.createDataFrame(
        [page("2025", 0, 30, 70), page("2025", 30, 30, 70)],
        PAGE_SCHEMA,
    )

    result = complete_jolpica_loads(df, ["_season"])

    assert result.collect() == []


def test_complete_jolpica_loads_counts_duplicate_page_once(spark):
    df = spark.createDataFrame(
        [page("2025", 0, 30, 70), page("2025", 0, 30, 70), page("2025", 30, 30, 70)],
        PAGE_SCHEMA,
    )

    result = complete_jolpica_loads(df, ["_season"])

    assert result.collect() == []

def test_complete_jolpica_loads_treats_empty_response_as_one_page(spark):
    df = spark.createDataFrame([page("2025", 0, 30, 0)], PAGE_SCHEMA)

    result = complete_jolpica_loads(df, ["_season"])

    assert [row["_season"] for row in result.collect()] == ["2025"]


def test_complete_jolpica_loads_checks_each_season_separately(spark):
    df = spark.createDataFrame(
        [
            page("2024", 0, 30, 20),
            page("2025", 0, 30, 70),
            page("2025", 30, 30, 70),
        ],
        PAGE_SCHEMA,
    )

    result = complete_jolpica_loads(df, ["_season"])

    assert [row["_season"] for row in result.collect()] == ["2024"]

def test_latest_complete_jolpica_ignores_load_without_marker(spark):
    df = spark.createDataFrame(
        [("2025", L1, "complete"), ("2025", L3, "unfinished")],
        JOLPICA_SCHEMA,
    )
    markers = spark.createDataFrame([marker(L1, seasons=[2025])], MARKER_SCHEMA)

    result = latest_complete_jolpica(df, markers)

    assert season_load_pairs(result) == [("2025", L1)]


def test_latest_complete_jolpica_takes_newest_marked_load_per_season(spark):
    df = spark.createDataFrame(
        [("2024", L1, "a"), ("2025", L1, "b"), ("2025", L2, "c")],
        JOLPICA_SCHEMA,
    )
    markers = spark.createDataFrame(
        [marker(L1, seasons=[2024, 2025]), marker(L2, seasons=[2025])],
        MARKER_SCHEMA,
    )

    result = latest_complete_jolpica(df, markers)

    assert season_load_pairs(result) == [("2024", L1), ("2025", L2)]


def test_latest_complete_jolpica_takes_seasonless_rows_from_newest_marked_load(spark):
    df = spark.createDataFrame(
        [("all", L1, "old_status"), ("all", L2, "new_status"), ("all", L3, "unfinished")],
        JOLPICA_SCHEMA,
    )
    markers = spark.createDataFrame(
        [marker(L1, seasons=[2025]), marker(L2, seasons=[2025])],
        MARKER_SCHEMA,
    )

    result = latest_complete_jolpica(df, markers)

    assert [row["value"] for row in result.collect()] == ["new_status"]


def test_all_complete_jolpica_keeps_every_marked_load(spark):
    df = spark.createDataFrame(
        [("2025", L1, "a"), ("2025", L2, "b"), ("2025", L3, "unfinished")],
        JOLPICA_SCHEMA,
    )
    markers = spark.createDataFrame(
        [marker(L1, seasons=[2025]), marker(L2, seasons=[2025])],
        MARKER_SCHEMA,
    )

    result = all_complete_jolpica(df, markers)

    assert season_load_pairs(result) == [("2025", L1), ("2025", L2)]


def test_latest_valid_openf1_takes_newest_marked_load_per_year(spark):
    df = spark.createDataFrame(
        [(2025, L1, "old"), (2025, L2, "new"), (2025, L3, "unfinished")],
        OPENF1_SCHEMA,
    )
    markers = spark.createDataFrame(
        [marker(L1, years=[2025]), marker(L2, years=[2025])],
        MARKER_SCHEMA,
    )

    result = latest_valid_openf1(df, markers)

    assert [row["value"] for row in result.collect()] == ["new"]