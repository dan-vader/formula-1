import re
from pathlib import Path

from formula_1.dq.bronze_checks import ERROR, WARN
from formula_1.dq.silver_checks import FOREIGN_KEYS, SILVER_KEYS, quarantine_table, silver_checks
from formula_1.silver.schemas import silver as silver_schemas

TRANSFORMATIONS_DIR = Path(__file__).parents[2] / "src" / "formula_1_silver" / "transformations"
REGISTER_PATTERN = re.compile(r'factory\.register\(\s*"(silver_\w+)"')
DDL_COLUMN = re.compile(r"(?:^|,)\s*(\w+)\s")


def table_columns(table):
    ddl = getattr(silver_schemas, table.removeprefix("silver_").upper() + "_DDL")
    return set(DDL_COLUMN.findall(ddl))


def registered_tables():
    tables = set()
    for path in TRANSFORMATIONS_DIR.glob("*.py"):
        tables.update(REGISTER_PATTERN.findall(path.read_text(encoding="utf-8")))
    return tables

def checks_for(table):
    return [c for c in silver_checks("c", "s") if c.table == f"c.s.{table}"]


def test_silver_keys_cover_every_registered_table():
    assert set(SILVER_KEYS) == registered_tables()


def test_silver_checks_use_fully_qualified_table_names():
    checks = silver_checks("f1_catalog", "silver")

    assert checks
    for check in checks:
        assert check.table.startswith("f1_catalog.silver.silver_")
        assert check.table in check.sql or quarantine_table(check.table) in check.sql


def test_silver_check_names_are_unique_per_table():
    pairs = [(check.table, check.name) for check in silver_checks("c", "s")]

    assert len(pairs) == len(set(pairs))

def test_silver_keys_exist_in_table_schemas():
    for table, key in SILVER_KEYS.items():
        missing = set(key) - table_columns(table)
        assert not missing, f"{table}: key columns not in DDL: {missing}"


def test_foreign_keys_exist_in_both_tables():
    for table, column, ref_table, _ in FOREIGN_KEYS:
        assert column in table_columns(table), f"{column} not in {table}"
        assert column in table_columns(ref_table), f"{column} not in {ref_table}"


def test_foreign_keys_point_between_known_tables_with_valid_severity():
    for table, _, ref_table, severity in FOREIGN_KEYS:
        assert table in SILVER_KEYS
        assert ref_table in SILVER_KEYS
        assert severity in {ERROR, WARN}


def test_foreign_key_check_counts_missing_parents():
    check = next(c for c in silver_checks("c", "s") if c.name == "driver_id_in_silver_driver")

    assert check.group == "consistency"
    assert "LEFT ANTI JOIN c.s.silver_driver" in check.sql
    assert "driver_id IS NOT NULL" in check.sql


def test_every_table_has_completeness_and_quarantine_checks():
    for table in SILVER_KEYS:
        names = {check.name for check in checks_for(table)}
        assert {"table_not_empty", "quarantine_empty", "quarantine_share_max_5pct"} <= names, table


def test_quarantine_checks_read_matching_quarantine_table():
    for check in checks_for("silver_race_result"):
        if check.name.startswith("quarantine"):
            assert "c.s.silver_quarantine_race_result" in check.sql


def test_quarantine_share_uses_given_threshold():
    checks = silver_checks("c", "s", max_quarantine_share=0.1)
    share = next(c for c in checks if c.name.startswith("quarantine_share"))

    assert share.name == "quarantine_share_max_10pct"
    assert "0.1 * (bad + good)" in share.sql


def test_session_driver_mapping_checks_use_existing_columns():
    assert {"driver_id", "acronym_match"} <= table_columns("silver_session_driver")