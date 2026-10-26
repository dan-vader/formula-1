import csv
from pathlib import Path

import pytest

REFERENCE_DIR = Path(__file__).parents[2] / "data" / "reference"

# file -> (columns the bronze pipeline expects, columns that must be unique together)
CONTRACT = {
    "ref_pu_supplier": (["constructor_id", "season", "power_unit_supplier"], ("constructor_id", "season")),
    "ref_team_lineage": (["constructor_id", "team_lineage"], ("constructor_id",)),
}


def read_reference(name):
    with open(REFERENCE_DIR / f"{name}.csv", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return reader.fieldnames, list(reader)


@pytest.mark.parametrize("name", CONTRACT)
def test_reference_file_has_contract_columns(name):
    columns, _ = read_reference(name)

    assert columns == CONTRACT[name][0]


@pytest.mark.parametrize("name", CONTRACT)
def test_reference_file_has_no_duplicate_keys(name):
    _, rows = read_reference(name)
    key_columns = CONTRACT[name][1]

    keys = [tuple(row[column] for column in key_columns) for row in rows]

    assert len(keys) == len(set(keys))


@pytest.mark.parametrize("name", CONTRACT)
def test_reference_file_has_no_empty_values(name):
    _, rows = read_reference(name)

    assert all(value.strip() for row in rows for value in row.values())


def test_every_supplier_constructor_has_lineage():
    _, suppliers = read_reference("ref_pu_supplier")
    _, lineage = read_reference("ref_team_lineage")

    assert {row["constructor_id"] for row in suppliers} <= {row["constructor_id"] for row in lineage}
