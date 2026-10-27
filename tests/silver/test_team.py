from datetime import datetime

from formula_1.silver.transforms import team
from formula_1.silver.transforms.team import team_deduped, team_enriched

TEAM_SCHEMA = "c struct<constructorId: string, name: string>, _season string, _load_id string, _ingested_at timestamp"
L1 = "20251201T090000Z"
L2 = "20251208T090000Z"
INGESTED = datetime(2025, 12, 8, 9, 0)
SUPPLIER_SCHEMA = "constructor_id string, season int, power_unit_supplier string"
LINEAGE_SCHEMA = "constructor_id string, team_lineage string"

def use_reference(spark, monkeypatch, suppliers, lineage):
    supplier_df = spark.createDataFrame(suppliers, SUPPLIER_SCHEMA)
    lineage_df = spark.createDataFrame(lineage, LINEAGE_SCHEMA)
    monkeypatch.setattr(team, "_supplier", lambda: supplier_df)
    monkeypatch.setattr(team, "_team_lineage", lambda: lineage_df)

def test_team_deduped_normalises_constructor_id_and_season(spark):
    df = spark.createDataFrame([((" Ferrari ", "Ferrari"), "2025", L1, INGESTED)], TEAM_SCHEMA)

    result = team_deduped(df).first()

    assert (result["constructor_id"], result["season"]) == ("ferrari", 2025)


def test_team_deduped_keeps_newest_load_per_constructor_and_season(spark):
    df = spark.createDataFrame(
        [
            (("ferrari", "Ferrari"), "2025", L1, INGESTED),
            (("Ferrari", "Scuderia Ferrari"), "2025", L2, INGESTED),
            (("ferrari", "Ferrari"), "2024", L1, INGESTED),
        ],
        TEAM_SCHEMA,
    )

    result = team_deduped(df)

    rows = {(row["constructor_id"], row["season"]): row["_load_id"] for row in result.collect()}
    assert rows == {("ferrari", 2025): L2, ("ferrari", 2024): L1}


def test_team_enriched_adds_supplier_and_lineage(spark, monkeypatch):
    use_reference(spark, monkeypatch, [("ferrari", 2025, "Ferrari")], [("ferrari", "Ferrari")])
    teams = spark.createDataFrame([(("Ferrari", " Ferrari "), "2025", L2, INGESTED)], TEAM_SCHEMA)

    result = team_enriched(team_deduped(teams)).first().asDict()

    assert result == {
        "constructor_id": "ferrari",
        "season": 2025,
        "power_unit_supplier": "Ferrari",
        "team_name": "Ferrari",
        "team_lineage": "Ferrari",
        "team_colour": None,
        "ingested_at": INGESTED,
        "_load_id": L2,
    }


def test_team_enriched_keeps_team_missing_in_reference(spark, monkeypatch):
    use_reference(spark, monkeypatch, [("ferrari", 2024, "Ferrari")], [])
    teams = spark.createDataFrame([(("ferrari", "Ferrari"), "2025", L2, INGESTED)], TEAM_SCHEMA)

    result = team_enriched(team_deduped(teams)).collect()

    assert len(result) == 1
    assert result[0]["power_unit_supplier"] is None
    assert result[0]["team_lineage"] is None


