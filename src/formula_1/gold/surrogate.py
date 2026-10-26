from functools import reduce

from pyspark.sql import Column, DataFrame, functions as F
from pyspark.sql.types import StringType

UNKNOWN_SK = -1
UNKNOWN_LABEL = "Unknown"


def _col(c: str | Column) -> Column:
    return F.col(c) if isinstance(c, str) else c


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
    values = []
    for field in dim.schema.fields:
        if field.name == sk_col:
            value = F.lit(UNKNOWN_SK)
        elif field.name in overrides:
            value = F.lit(overrides[field.name])
        elif isinstance(field.dataType, StringType):
            value = F.lit(UNKNOWN_LABEL)
        else:
            value = F.lit(None)
        values.append(value.cast(field.dataType).alias(field.name))
    return dim.unionByName(dim.sparkSession.range(1).select(*values))