from itertools import product

import pytest
from pyspark.sql import functions as F

from formula_1.gold.surrogate import UNKNOWN_SK, sk
from formula_1.gold.transforms.dim_junk import (
    LAP_FLAGS,
    PIT_STOP_FLAGS,
    dim_lap_flags,
    dim_pit_stop_flags,
)

JUNK_DIMS = [
    pytest.param(dim_lap_flags, LAP_FLAGS, "lap_flags_sk", id="lap_flags"),
    pytest.param(dim_pit_stop_flags, PIT_STOP_FLAGS, "pit_stop_flags_sk", id="pit_stop_flags"),
]


def all_combinations(flags):
    return list(product([False, True], repeat=len(flags)))


def flags_schema(flags):
    return ", ".join(f"{flag} boolean" for flag in flags)


@pytest.mark.parametrize(("build", "flags", "sk_col"), JUNK_DIMS)
def test_junk_dim_has_every_combination_once_plus_unknown(spark, build, flags, sk_col):
    dim = build()

    known = dim.where(F.col(sk_col) != UNKNOWN_SK).collect()
    combos = [tuple(row[flag] for flag in flags) for row in known]

    assert sorted(combos) == sorted(all_combinations(flags))
    assert dim.count() == len(combos) + 1


@pytest.mark.parametrize(("build", "flags", "sk_col"), JUNK_DIMS)
def test_junk_dim_keys_are_unique(spark, build, flags, sk_col):
    dim = build()

    assert dim.select(sk_col).distinct().count() == dim.count()


@pytest.mark.parametrize(("build", "flags", "sk_col"), JUNK_DIMS)
def test_fact_flags_find_their_row_in_junk_dim(spark, build, flags, sk_col):
    facts = spark.createDataFrame(all_combinations(flags), flags_schema(flags))
    fact_keys = facts.select(sk(*flags).alias(sk_col))

    missing = fact_keys.join(build(), sk_col, "left_anti")

    assert missing.count() == 0


@pytest.mark.parametrize(("build", "flags", "sk_col"), JUNK_DIMS)
def test_fact_with_missing_flag_points_to_unknown_row(spark, build, flags, sk_col):
    row = tuple([None] + [True] * (len(flags) - 1))
    facts = spark.createDataFrame([row], flags_schema(flags))
    fact_keys = facts.select(sk(*flags).alias(sk_col))

    assert fact_keys.first()[sk_col] == UNKNOWN_SK
    assert fact_keys.join(build(), sk_col, "left_anti").count() == 0