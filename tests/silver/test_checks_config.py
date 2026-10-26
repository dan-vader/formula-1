from collections import Counter
from pathlib import Path

import pytest
import yaml

CHECKS_DIR = Path(__file__).parents[2] / "src" / "formula_1" / "silver" / "checks"
CHECK_FILES = sorted(CHECKS_DIR.glob("*.yml"))
CRITICALITIES = {"error", "warn"}

def load(path):
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)
    

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