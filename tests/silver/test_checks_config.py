import re
from collections import Counter
from pathlib import Path

import pytest
import yaml

CHECKS_DIR = Path(__file__).parents[2] / "src" / "formula_1" / "silver" / "checks"
CHECK_FILES = sorted(CHECKS_DIR.glob("*.yml"))
CRITICALITIES = {"error", "warn"}
TRANSFORMATIONS_DIR = Path(__file__).parents[2] / "src" / "formula_1_silver" / "transformations"
REGISTER_PATTERN = re.compile(r'factory\.register\(\s*"(silver_\w+)"')

def load(path):
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def registered_tables():
    tables = set()
    for path in TRANSFORMATIONS_DIR.glob("*.py"):
        tables.update(REGISTER_PATTERN.findall(path.read_text(encoding="utf-8")))
    return tables
    

def test_checks_folder_is_not_empty():
    assert CHECK_FILES


@pytest.mark.parametrize("path", CHECK_FILES, ids=lambda p: p.stem)
def test_checks_file_is_non_empty_list(path):
    checks = load(path)

    assert isinstance(checks, list)
    assert checks

@pytest.mark.parametrize("path", CHECK_FILES, ids=lambda p: p.stem)
def test_every_check_has_name_criticality_and_function(path):
    for check in load(path):
        name = check.get("name")

        assert name, f"rule without name: {check}"
        assert check.get("criticality") in CRITICALITIES, f"{name}: bad criticality"
        assert check.get("check", {}).get("function"), f"{name}: no check.function"

@pytest.mark.parametrize("path", CHECK_FILES, ids=lambda p: p.stem)
def test_check_names_are_unique(path):
    names = [check["name"] for check in load(path)]

    duplicates = [name for name, count in Counter(names).items() if count > 1]

    assert not duplicates, f"duplicate rule names: {duplicates}"


def test_transformations_register_tables():
    assert registered_tables()


def test_every_registered_table_has_checks_file():
    missing = registered_tables() - {path.stem for path in CHECK_FILES}

    assert not missing, f"tables without checks yml: {sorted(missing)}"


def test_every_checks_file_belongs_to_registered_table():
    unused = {path.stem for path in CHECK_FILES} - registered_tables()

    assert not unused, f"checks yml without registered table: {sorted(unused)}"