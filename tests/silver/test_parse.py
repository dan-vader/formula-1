import json
from datetime import datetime

from formula_1.silver.transforms.parse import explode_openf1, explode_results

OPENF1_BRONZE_SCHEMA = "payload string, _load_id string, _ingested_at timestamp, _year int, _session_key int"
JOLPICA_BRONZE_SCHEMA = "payload string, _load_id string, _ingested_at timestamp, _season string, _round string"
LAP_DDL = "driver_number string, lap_number string"
LOAD_ID = "20251208T090000Z"
INGESTED = datetime(2025, 12, 8, 9, 0)


def openf1_row(payload):
    return (payload, LOAD_ID, INGESTED, 2025, 9839)


def jolpica_row(*races):
    payload = {"MRData": {"total": "1", "limit": "30", "RaceTable": {"Races": list(races)}}}
    return (json.dumps(payload), LOAD_ID, INGESTED, "2025", "all")


def race(round_number, results=(), sprint_results=()):
    return {
        "season": "2025",
        "round": str(round_number),
        "Results": [{"Driver": {"driverId": driver_id}} for driver_id in results],
        "SprintResults": [{"Driver": {"driverId": driver_id}} for driver_id in sprint_results],
    }


def test_explode_openf1_returns_one_row_per_record_with_tech_columns(spark):
    payload = json.dumps([{"driver_number": "44", "lap_number": "1"}, {"driver_number": "44", "lap_number": "2"}])
    df = spark.createDataFrame([openf1_row(payload)], OPENF1_BRONZE_SCHEMA)

    result = explode_openf1(df, LAP_DDL)

    assert result.columns == ["driver_number", "lap_number", "_load_id", "_ingested_at", "_year", "_session_key"]
    assert sorted(row["lap_number"] for row in result.collect()) == ["1", "2"]


def test_explode_openf1_keeps_only_schema_fields(spark):
    payload = json.dumps([{"driver_number": 44, "unknown_field": "x"}])
    df = spark.createDataFrame([openf1_row(payload)], OPENF1_BRONZE_SCHEMA)

    row = explode_openf1(df, LAP_DDL).first()

    assert row["driver_number"] == "44"
    assert row["lap_number"] is None
    assert "unknown_field" not in row.asDict()


def test_explode_openf1_drops_payload_that_is_not_json(spark):
    df = spark.createDataFrame([openf1_row("not json")], OPENF1_BRONZE_SCHEMA)

    result = explode_openf1(df, LAP_DDL)

    # Current behaviour: a broken payload disappears silently, bronze DQ (completeness) must catch it
    assert result.collect() == []

def test_explode_results_returns_one_row_per_result_in_every_race(spark):
    df = spark.createDataFrame(
        [jolpica_row(race(1, results=["verstappen", "norris"]), race(2, results=["leclerc"]))],
        JOLPICA_BRONZE_SCHEMA,
    )

    result = explode_results(df)

    rows = sorted((row["Race"]["round"], row["Result"]["Driver"]["driverId"]) for row in result.collect())
    assert rows == [("1", "norris"), ("1", "verstappen"), ("2", "leclerc")]


def test_explode_results_reads_sprint_results_when_asked(spark):
    df = spark.createDataFrame(
        [jolpica_row(race(1, results=["verstappen"], sprint_results=["piastri"]))],
        JOLPICA_BRONZE_SCHEMA,
    )

    result = explode_results(df, list_field="SprintResults")

    assert [row["Result"]["Driver"]["driverId"] for row in result.collect()] == ["piastri"]