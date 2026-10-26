from functools import reduce

from pyspark.sql import Column, DataFrame, functions as F
from pyspark.sql.types import StringType

UNKNOWN_SK = -1
UNKNOWN_LABEL = "Unknown"


def _col(c: str | Column) -> Column:
    return F.col(c) if isinstance(c, str) else c


def _placeholder(field, overrides: dict) -> Column:
    if field.name in overrides:
        return _col(overrides[field.name])
    if isinstance(field.dataType, StringType):
        return F.lit(UNKNOWN_LABEL)
    return F.lit(None)


def sk(*natural_key: str | Column) -> Column:
    """Surrogate key: xxhash64 of the natural key, UNKNOWN_SK if any part is NULL."""
    parts = [_col(c) for c in natural_key]
    any_null = reduce(lambda a, b: a | b, [p.isNull() for p in parts])
    hashed = F.xxhash64(*[p.cast("string") for p in parts])
    return F.when(any_null, F.lit(UNKNOWN_SK)).otherwise(hashed)


def date_sk(date: str | Column) -> Column:
    """Date key as YYYYMMDD, UNKNOWN_SK if the date is NULL."""
    yyyymmdd = F.date_format(F.to_date(_col(date)), "yyyyMMdd").cast("int")
    return F.coalesce(yyyymmdd, F.lit(UNKNOWN_SK))


def with_unknown_member(dim: DataFrame, sk_col: str, **overrides) -> DataFrame:
    """Append the unknown member: sk = -1, strings = 'Unknown', others NULL unless overridden."""
    overrides = {sk_col: F.lit(UNKNOWN_SK), **{k: F.lit(v) for k, v in overrides.items()}}
    values = [_placeholder(f, overrides).cast(f.dataType).alias(f.name) for f in dim.schema.fields]
    return dim.unionByName(dim.sparkSession.range(1).select(*values))


def with_inferred_members(known: DataFrame, referenced: DataFrame, key: str, **overrides) -> DataFrame:
    missing = referenced.where(F.col(key).isNotNull()).join(known, key, "left_anti")
    overrides = {key: key, "is_inferred": F.lit(True), "inferred_at": "inferred_at", **overrides}
    values = [_placeholder(f, overrides).cast(f.dataType).alias(f.name) for f in known.schema.fields]
    return known.unionByName(missing.select(*values))