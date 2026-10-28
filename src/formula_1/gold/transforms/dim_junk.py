from itertools import product

from pyspark.sql import DataFrame

from formula_1.gold.surrogate import sk, with_unknown_member
from formula_1.silver.config import _spark

LAP_FLAGS = ["is_pit_in_lap", "is_pit_out_lap"]
PIT_STOP_FLAGS = ["is_tyre_change", "is_penalty_served"]


def _junk(flags: list[str], sk_col: str) -> DataFrame:
    """Every combination of the flags (2^n rows), plus the unknown member."""
    schema = ", ".join(f"{flag} boolean" for flag in flags)
    rows = _spark().createDataFrame(list(product([False, True], repeat=len(flags))), schema)
    dim = rows.select(sk(*flags).alias(sk_col), *flags)
    return with_unknown_member(dim, sk_col)


def dim_lap_flags() -> DataFrame:
    return _junk(LAP_FLAGS, "lap_flags_sk")


def dim_pit_stop_flags() -> DataFrame:
    return _junk(PIT_STOP_FLAGS, "pit_stop_flags_sk")