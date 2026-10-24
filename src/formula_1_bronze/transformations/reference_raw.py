import csv
from pathlib import Path

from pyspark import pipelines as dp
from pyspark.sql import functions as F

REFERENCE_PATH = Path(spark.conf.get("formula_1.reference_path"))

REFERENCE_TABLES = {
    "ref_pu_supplier": ["constructor_id", "season", "power_unit_supplier"],
    "ref_team_lineage": ["constructor_id", "team_lineage"],
}


def define_reference_table(name, columns):
    @dp.table(
        name=name,
        comment=f"Reference data from data/reference/{name}.csv, loaded as is",
    )
    def reference():
        with open(REFERENCE_PATH / f"{name}.csv", newline="", encoding="utf-8") as f:
            rows = [tuple(row[c] for c in columns) for row in csv.DictReader(f)]
        schema = ", ".join(f"{c} STRING" for c in columns)
        return spark.createDataFrame(rows, schema).withColumn("_ingested_at", F.current_timestamp())


for name, columns in REFERENCE_TABLES.items():
    define_reference_table(name, columns)
