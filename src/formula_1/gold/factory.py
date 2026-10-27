import re
from collections.abc import Callable
from importlib.resources import files

from databricks.labs.dqx.config import FileChecksStorageConfig
from pyspark import pipelines as dp
from pyspark.sql import DataFrame, functions as F

from formula_1.gold.config import gold_table
from formula_1.silver.config import field_names
from formula_1.silver.transforms.dq import _engine

# Key constraints are for Unity Catalog only; Spark's schema parser does not accept them
_KEY_CONSTRAINTS = re.compile(r",\s*CONSTRAINT\s+\w+\s+PRIMARY KEY\s*\([^)]*\)|\s+PRIMARY KEY\b", re.IGNORECASE)


def columns(ddl: str) -> list[str]:
    return field_names(_KEY_CONSTRAINTS.sub("", ddl))


def register_dimension(table: str, build: Callable[[], DataFrame], ddl: str, comment: str):
    cols = columns(ddl)
    checks_file = str(files("formula_1.gold") / "checks" / f"{table}.yml")

    @dp.temporary_view(name=f"{table}_checked")
    @dp.expect_all_or_fail({"dq_passed": "_errors IS NULL"})
    def _checked():
        df = build()
        engine = _engine(df)
        checks = engine.load_checks(config=FileChecksStorageConfig(location=checks_file))
        return engine.apply_checks_by_metadata(df, checks)

    @dp.materialized_view(name=table, schema=ddl, comment=comment, cluster_by_auto=True)
    def _dimension():
        return dp.read(f"{table}_checked").select(*cols)


def register_fact(
    table: str,
    build: Callable[[], DataFrame],
    ddl: str,
    comment: str,
    grain: list[str],
    references: dict[str, str],
    warnings: dict[str, str] | None = None,
):
    cols = columns(ddl)
    foreign_keys = "".join(
        f", CONSTRAINT {table}_{col}_fk FOREIGN KEY ({col}) REFERENCES {gold_table(ref.split('.')[0])}({ref.split('.')[1]})"
        for col, ref in references.items()
    )
    keys_not_null = {f"{c}_not_null": f"{c} IS NOT NULL" for c in [*grain, *references]}
    keys_known = {f"{c}_known": f"{c} <> -1" for c in references}

    def _fact():
        return build().select(*cols)

    _fact = dp.expect_all({**keys_known, **(warnings or {})})(_fact)
    _fact = dp.expect_all_or_fail(keys_not_null)(_fact)
    dp.materialized_view(name=table, schema=ddl + foreign_keys, comment=comment, cluster_by_auto=True)(_fact)

    @dp.materialized_view(name=f"{table}_grain_check", private=True)
    @dp.expect_or_fail("unique_grain", "num_entries = 1")
    def _grain_check():
        return dp.read(table).groupBy(*grain).agg(F.count(F.lit(1)).alias("num_entries"))