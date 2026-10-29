import re
from datetime import date
from pathlib import Path

import pytest

from formula_1.dq.bronze_checks import ERROR, WARN
from formula_1.dq.gold_checks import (
    DIMENSION_KEYS,
    FACT_REFERENCES,
    contiguous_team_versions,
    gold_checks,
    key_in_dimension,
    one_current_team_version,
    unknown_member_once,
    unknown_share,
)
from formula_1.gold.schemas import gold as gold_schemas

TRANSFORMATIONS_DIR = Path(__file__).parents[2] / "src" / "formula_1_gold" / "transformations"
DIMENSION_PATTERN = re.compile(r'register_dimension\(\s*"(dim_\w+)"')
FACT_PATTERN = re.compile(r'register_fact\(\s*"(\w+)"')
REFERENCE_PATTERN = re.compile(r'"(\w+)":\s*"(dim_\w+)\.(\w+)"')

TEAM_SCHEMA = "team_sk bigint, constructor_id string, effective_from date, effective_to date, is_current boolean"
UNKNOWN_TEAM = (-1, "Unknown", None, None, False)
OPEN_END = date(9999, 12, 31)


def transformation_sources():
    return [path.read_text(encoding="utf-8") for path in TRANSFORMATIONS_DIR.glob("*.py")]


def registered_dimensions():
    return {dim for source in transformation_sources() for dim in DIMENSION_PATTERN.findall(source)}


def registered_facts():
    facts = {}
    for source in transformation_sources():
        for fact in FACT_PATTERN.findall(source):
            facts[fact] = {column: (dim, key) for column, dim, key in REFERENCE_PATTERN.findall(source)}
    return facts


def failed_rows(spark, check, **tables):
    for name, df in tables.items():
        df.createOrReplaceTempView(name)
    return spark.sql(check.sql).first()["failed_rows"]


def test_dimension_keys_cover_every_registered_dimension():
    assert set(DIMENSION_KEYS) == registered_dimensions()


def test_dimension_key_is_first_column_of_its_ddl():
    for dim, key in DIMENSION_KEYS.items():
        ddl = getattr(gold_schemas, dim.upper() + "_DDL")
        assert ddl.startswith(f"{key} "), dim


def test_fact_references_match_gold_pipeline():
    registered = registered_facts()

    assert set(FACT_REFERENCES) == set(registered)
    for fact, references in registered.items():
        assert FACT_REFERENCES[fact] == {column: dim for column, (dim, _) in references.items()}, fact
        for dim, key in references.values():
            assert DIMENSION_KEYS[dim] == key, f"{fact}: {dim}.{key}"


def test_gold_checks_use_fully_qualified_and_unique_names():
    checks = gold_checks("c", "g")
    pairs = [(check.table, check.name) for check in checks]

    assert all(check.table.startswith("c.g.") for check in checks)
    assert len(pairs) == len(set(pairs))


def test_every_fact_key_has_dimension_and_unknown_share_checks():
    checks = gold_checks("c", "g")

    for fact, references in FACT_REFERENCES.items():
        names = {check.name for check in checks if check.table == f"c.g.{fact}"}
        assert "table_not_empty" in names, fact
        for column, dim in references.items():
            assert {f"{column}_in_{dim}", f"{column}_unknown_share_max_5pct"} <= names, f"{fact}.{column}"


def test_dangling_key_is_error_and_unknown_share_is_warning():
    assert key_in_dimension("f", "driver_sk", "d", "driver_sk").severity == ERROR
    assert unknown_share("f", "driver_sk", 0.05).severity == WARN


@pytest.mark.parametrize(
    ("keys", "expected"),
    [
        pytest.param([-1, 5], 0, id="one_unknown"),
        pytest.param([5], 1, id="no_unknown"),
        pytest.param([-1, -1, 5], 1, id="two_unknown"),
    ],
)
def test_unknown_member_once(spark, keys, expected):
    dim = spark.createDataFrame([(k,) for k in keys], "driver_sk bigint")

    assert failed_rows(spark, unknown_member_once("dim", "driver_sk"), dim=dim) == expected


def test_key_in_dimension_counts_only_dangling_keys(spark):
    fact = spark.createDataFrame([(1,), (1,), (2,), (99,), (-1,)], "finish_status_sk bigint")
    dim = spark.createDataFrame([(-1,), (1,), (2,)], "finish_status_sk bigint")
    check = key_in_dimension("fact", "finish_status_sk", "dim", "finish_status_sk")

    assert failed_rows(spark, check, fact=fact, dim=dim) == 1


@pytest.mark.parametrize(
    ("unknown", "known", "expected"),
    [
        pytest.param(1, 9, 1, id="10pct_is_too_many"),
        pytest.param(1, 29, 0, id="3pct_is_fine"),
    ],
)
def test_unknown_share(spark, unknown, known, expected):
    fact = spark.createDataFrame([(-1,)] * unknown + [(7,)] * known, "tyre_compound_sk bigint")

    assert failed_rows(spark, unknown_share("fact", "tyre_compound_sk", 0.05), fact=fact) == expected


@pytest.mark.parametrize(
    ("versions", "expected"),
    [
        pytest.param(
            [(1, "mclaren", date(2020, 1, 1), date(2020, 12, 31), False),
             (2, "mclaren", date(2021, 1, 1), OPEN_END, True)],
            0,
            id="contiguous",
        ),
        pytest.param(
            [(1, "mclaren", date(2020, 1, 1), date(2020, 6, 30), False),
             (2, "mclaren", date(2021, 1, 1), OPEN_END, True)],
            1,
            id="gap",
        ),
        pytest.param(
            [(1, "mclaren", date(2020, 1, 1), date(2021, 6, 30), False),
             (2, "mclaren", date(2021, 1, 1), OPEN_END, True)],
            1,
            id="overlap",
        ),
    ],
)
def test_contiguous_team_versions(spark, versions, expected):
    dim = spark.createDataFrame([*versions, UNKNOWN_TEAM], TEAM_SCHEMA)

    assert failed_rows(spark, contiguous_team_versions("dim_team"), dim_team=dim) == expected


@pytest.mark.parametrize(
    ("versions", "expected"),
    [
        pytest.param(
            [(1, "mclaren", date(2020, 1, 1), date(2020, 12, 31), False),
             (2, "mclaren", date(2021, 1, 1), OPEN_END, True)],
            0,
            id="one_current",
        ),
        pytest.param(
            [(1, "mclaren", date(2020, 1, 1), date(2020, 12, 31), True),
             (2, "mclaren", date(2021, 1, 1), OPEN_END, True)],
            1,
            id="two_current",
        ),
        pytest.param(
            [(3, "racing_point", date(2020, 1, 1), date(2020, 12, 31), False)],
            0,
            id="retired_team_has_none",
        ),
    ],
)
def test_one_current_team_version(spark, versions, expected):
    dim = spark.createDataFrame([*versions, UNKNOWN_TEAM], TEAM_SCHEMA)

    assert failed_rows(spark, one_current_team_version("dim_team"), dim_team=dim) == expected