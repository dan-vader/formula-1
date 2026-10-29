from pyspark.sql import SparkSession, DataFrame


JOLPICA_LOADS = "jolpica_loads_raw"
OPENF1_LOADS = "openf1_loads_raw"

JOLPICA_TECH = ["_load_id", "_ingested_at", "_season", "_round"]
OPENF1_TECH = ["_load_id", "_ingested_at", "_year", "_session_key"]

CSV_PU_SUPPLIER = "ref_pu_supplier"
CSV_TEAM_LINEAGE = "ref_team_lineage"

QUARANTINED_TABLES = [
    "silver_finish_status", "silver_driver", "silver_team",
    "silver_session", "silver_session_driver", "silver_race_result",
    "silver_jolpica_pit_stop", "silver_race_control", "silver_track_status_interval",
    "silver_stint", "silver_lap", "silver_pit_stop", "silver_overtake",
]


def _spark() -> SparkSession:
    return SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()


def bronze(table: str) -> DataFrame:
    catalog = _spark().conf.get("formula_1.catalog")
    bronze = _spark().conf.get("formula_1.bronze_schema")
    return _spark().read.table(f"{catalog}.{bronze}.{table}")


def bronze_stream() -> DataFrame:
    return bronze("openf1_stream_raw")


def field_names(ddl: str) -> list[str]:
    return _spark().createDataFrame([], ddl).columns
