import re
from collections import Counter
from pathlib import Path

import pytest
import yaml
from pyspark.sql import functions as F

from formula_1.gold.schemas import gold as gold_schemas
from formula_1.gold.surrogate import with_unknown_member

CHECKS_DIR = Path(__file__).parents[2] / "src" / "formula_1" / "gold" / "checks"
CHECK_FILES = sorted(CHECKS_DIR.glob("*.yml"))
CRITICALITIES = {"error", "warn"}
PRIMARY_KEY = re.compile(r"\s+NOT NULL PRIMARY KEY", re.IGNORECASE)


def load(path):
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def all_checks():
    return [(path.stem, check) for path in CHECK_FILES for check in load(path)]


def sql_checks():
    return [
        pytest.param(table, check, id=check["name"])
        for table, check in all_checks()
        if check["check"]["function"] == "sql_expression"
    ]


def dimensions_in_schemas():
    return {
        name.removesuffix("_DDL").lower()
        for name in dir(gold_schemas)
        if name.startswith("DIM_") and name.endswith("_DDL")
    }


def unknown_row(spark, table):
    ddl = PRIMARY_KEY.sub("", getattr(gold_schemas, table.upper() + "_DDL"))
    empty = spark.createDataFrame([], ddl)
    return with_unknown_member(empty, empty.columns[0])


def test_every_dimension_has_checks_file():
    assert dimensions_in_schemas() == {path.stem for path in CHECK_FILES}


@pytest.mark.parametrize("path", CHECK_FILES, ids=lambda p: p.stem)
def test_every_check_has_name_criticality_and_function(path):
    for check in load(path):
        name = check.get("name")

        assert name, f"rule without name: {check}"
        assert check.get("criticality") in CRITICALITIES, f"{name}: bad criticality"
        assert check.get("check", {}).get("function"), f"{name}: no check.function"


def test_rule_names_are_unique_across_gold():
    names = [check["name"] for _, check in all_checks()]

    duplicates = [name for name, count in Counter(names).items() if count > 1]

    assert not duplicates, f"duplicate rule names: {duplicates}"


@pytest.mark.parametrize(("table", "check"), sql_checks())
def test_sql_rule_lets_unknown_row_pass(spark, table, check):
    expression = check["check"]["arguments"]["expression"]

    result = unknown_row(spark, table).select(F.expr(expression).alias("ok")).first()["ok"]

    assert result is True, f"{check['name']} fails on the Unknown row of {table}"