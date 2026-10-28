import json
from datetime import datetime

from formula_1.silver.transforms.dq import quarantine

CHECKED_SCHEMA = (
    "driver_id string, season int, ingested_at timestamp, _load_id string, "
    "_errors array<struct<name: string, message: string>>, "
    "_warnings array<struct<name: string, message: string>>"
)
INGESTED = datetime(2025, 12, 8, 9, 0)
L1 = "20251201T090000Z"
L2 = "20251208T090000Z"


def error(name):
    return (name, f"{name} failed")


def test_quarantine_keeps_only_rows_with_errors(spark):
    df = spark.createDataFrame(
        [
            ("hamilton", 2025, INGESTED, L1, None, [error("R-W-01")]),
            ("verstappen", 2025, INGESTED, L1, [error("R-1"), error("R-2")], None),
        ],
        CHECKED_SCHEMA,
    )

    rows = quarantine(df, "silver_driver", ["driver_id", "season"]).collect()

    assert len(rows) == 1
    row = rows[0]
    assert row["source_table"] == "silver_driver"
    assert row["record_key"] == "verstappen|2025"
    assert row["failed_rules"] == ["R-1", "R-2"]
    assert row["source_load_id"] == L1
    assert row["source_ingested_at"] == INGESTED
    assert set(json.loads(row["payload"])) == {"driver_id", "season", "ingested_at", "_load_id"}


def test_quarantine_record_key_keeps_position_of_missing_key_part(spark):
    df = spark.createDataFrame([(None, 2025, INGESTED, L1, [error("R-1")], None)], CHECKED_SCHEMA)

    row = quarantine(df, "silver_driver", ["driver_id", "season"]).first()

    assert row["record_key"] == "|2025"


def test_quarantine_id_is_stable_and_depends_on_load(spark):
    df = spark.createDataFrame(
        [
            ("verstappen", 2025, INGESTED, L1, [error("R-1")], None),
            ("verstappen", 2025, INGESTED, L1, [error("R-1")], None),
            ("verstappen", 2025, INGESTED, L2, [error("R-1")], None),
        ],
        CHECKED_SCHEMA,
    )

    ids = [row["quarantine_id"] for row in quarantine(df, "silver_driver", ["driver_id"]).collect()]

    assert len(set(ids)) == 2