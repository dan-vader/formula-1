import re
from pathlib import Path

from formula_1.dq.silver_checks import SILVER_KEYS, silver_checks

TRANSFORMATIONS_DIR = Path(__file__).parents[2] / "src" / "formula_1_silver" / "transformations"
REGISTER_PATTERN = re.compile(r'factory\.register\(\s*"(silver_\w+)"')


def registered_tables():
    tables = set()
    for path in TRANSFORMATIONS_DIR.glob("*.py"):
        tables.update(REGISTER_PATTERN.findall(path.read_text(encoding="utf-8")))
    return tables


def test_silver_keys_cover_every_registered_table():
    assert set(SILVER_KEYS) == registered_tables()


def test_silver_checks_use_fully_qualified_table_names():
    checks = silver_checks("f1_catalog", "silver")

    assert checks
    for check in checks:
        assert check.table.startswith("f1_catalog.silver.silver_")
        assert check.table in check.sql


def test_silver_check_names_are_unique_per_table():
    pairs = [(check.table, check.name) for check in silver_checks("c", "s")]

    assert len(pairs) == len(set(pairs))