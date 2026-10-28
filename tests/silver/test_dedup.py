from datetime import datetime

from chispa import assert_df_equality

from formula_1.silver.transforms.dedup import dedup_batch_over_stream, dedup_latest

SCHEMA = "id string, value string, _load_id string, _ingested_at timestamp"
INGESTED = datetime(2025, 12, 8, 9, 0)
LOAD_ID = "20251208T090000Z"
TEAM_SCHEMA = "constructor_id string, season int, value string, _load_id string, _ingested_at timestamp"
DRIVER_SCHEMA = "id string, value string, _season_i int, _load_id string, _ingested_at timestamp"
SOURCE_SCHEMA = "id string, value string, _is_batch boolean, _load_id string, _ingested_at timestamp"

def assert_rows(spark, result, rows, schema):
    expected = spark.createDataFrame(rows, schema)
    assert_df_equality(result, expected, ignore_row_order=True, ignore_nullable=True)


def test_dedup_latest_keeps_row_from_newest_load(spark):
    df = spark.createDataFrame(
        [
            ("a", "old", "20251201T090000Z", INGESTED),
            ("a", "new", "20251208T090000Z", INGESTED),
        ],
        SCHEMA,
    )

    result = dedup_latest(df, ["id"])

    assert_rows(spark, result, [("a", "new", "20251208T090000Z", INGESTED)], SCHEMA)


def test_dedup_latest_uses_ingested_at_when_load_id_is_equal(spark):
    df = spark.createDataFrame(
        [
            ("a", "first", LOAD_ID, datetime(2025, 12, 8, 9, 0)),
            ("a", "second", LOAD_ID, datetime(2025, 12, 8, 10, 0)),
        ],
        SCHEMA,
    )

    result = dedup_latest(df, ["id"])

    assert_rows(spark, result, [("a", "second", LOAD_ID, datetime(2025, 12, 8, 10, 0))], SCHEMA)


def test_dedup_latest_puts_rows_without_load_id_last(spark):
    df = spark.createDataFrame(
        [
            ("a", "no_load", None, datetime(2025, 12, 9, 9, 0)),
            ("a", "old", "20251201T090000Z", INGESTED),
        ],
        SCHEMA,
    )

    result = dedup_latest(df, ["id"])

    assert_rows(spark, result, [("a", "old", "20251201T090000Z", INGESTED)], SCHEMA)


def test_dedup_latest_keeps_one_row_per_key(spark):
    df = spark.createDataFrame(
        [
            ("a", "old", "20251201T090000Z", INGESTED),
            ("a", "new", "20251208T090000Z", INGESTED),
            ("b", "only", "20251201T090000Z", INGESTED),
        ],
        SCHEMA,
    )

    result = dedup_latest(df, ["id"])

    assert_rows(spark, result, [("a", "new", "20251208T090000Z", INGESTED), ("b", "only", "20251201T090000Z", INGESTED)], SCHEMA)


def test_dedup_latest_with_composite_key_keeps_each_season(spark):
    df = spark.createDataFrame(
        [
            ("ferrari", 2024, "old", "20251201T090000Z", INGESTED),
            ("ferrari", 2024, "new", "20251208T090000Z", INGESTED),
            ("ferrari", 2025, "only", "20251201T090000Z", INGESTED),
        ],
        TEAM_SCHEMA,
    )

    result = dedup_latest(df, ["constructor_id", "season"])

    assert_rows(spark, result, [("ferrari", 2024, "new", "20251208T090000Z", INGESTED), ("ferrari", 2025, "only", "20251201T090000Z", INGESTED)], TEAM_SCHEMA)


def test_dedup_latest_keeps_columns_and_drops_helper(spark):
    df = spark.createDataFrame([("a", "only", "20251201T090000Z", INGESTED)], SCHEMA)

    result = dedup_latest(df, ["id"])

    assert_df_equality(result, df, ignore_nullable=True)


def test_dedup_latest_respects_custom_order_by(spark):
    df = spark.createDataFrame(
        [
            ("hamilton", "season_2024", 2024, "20251208T090000Z", INGESTED),
            ("hamilton", "season_2025", 2025, "20251201T090000Z", INGESTED),
        ],
        DRIVER_SCHEMA,
    )

    result = dedup_latest(df, ["id"], order_by=("_season_i", "_load_id", "_ingested_at"))

    assert_rows(spark, result, [("hamilton", "season_2025", 2025, "20251201T090000Z", INGESTED)], DRIVER_SCHEMA)


def test_dedup_batch_over_stream_prefers_batch_even_if_stream_is_newer(spark):
    df = spark.createDataFrame(
        [
            ("lap_1", "stream", False, "20251209T090000Z", datetime(2025, 12, 9, 9, 0)),
            ("lap_1", "batch", True, "20251208T090000Z", INGESTED),
        ],
        SOURCE_SCHEMA,
    )

    result = dedup_batch_over_stream(df, ["id"])

    assert_rows(spark, result, [("lap_1", "batch", True, "20251208T090000Z", INGESTED)], SOURCE_SCHEMA)


def test_dedup_batch_over_stream_keeps_newest_among_batch_rows(spark):
    df = spark.createDataFrame(
        [
            ("lap_1", "old_batch", True, "20251201T090000Z", INGESTED),
            ("lap_1", "new_batch", True, "20251208T090000Z", INGESTED),
            ("lap_1", "stream", False, "20251209T090000Z", INGESTED),
        ],
        SOURCE_SCHEMA,
    )

    result = dedup_batch_over_stream(df, ["id"])

    assert_rows(spark, result, [("lap_1", "new_batch", True, "20251208T090000Z", INGESTED)], SOURCE_SCHEMA)