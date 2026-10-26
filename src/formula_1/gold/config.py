from pyspark.sql import DataFrame

from formula_1.silver.config import _spark


def silver(table: str) -> DataFrame:
    catalog = _spark().conf.get("formula_1.catalog")
    schema = _spark().conf.get("formula_1.silver_schema")
    return _spark().read.table(f"{catalog}.{schema}.{table}")


def seasons() -> list[int]:
    raw = _spark().conf.get("formula_1.seasons")
    return sorted(int(s) for s in raw.split(","))