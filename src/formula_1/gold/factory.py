import re
from collections.abc import Callable

from pyspark import pipelines as dp
from pyspark.sql import DataFrame

from formula_1.silver.config import field_names
from formula_1.silver.transforms.dq import _engine


_KEY_CONSTRAINTS = re.compile(r",\s*CONSTRAINT\s+\w+\s+PRIMARY KEY\s*\([^)]*\)|\s+PRIMARY KEY\b", re.IGNORECASE)


def columns(ddl: str) -> list[str]:
    return field_names(_KEY_CONSTRAINTS.sub("", ddl))


def _is_unique(table: str, rule: str, cols: list[str]) -> dict:
    return {
        "name": f"{table}_{rule}_unique",
        "criticality": "error",
        "check": {"function": "is_unique", "arguments": {"columns": cols}},
    }


def register_dimension(
    table: str,
    build: Callable[[], DataFrame],
    ddl: str,
    comment: str,
    sk_col: str,
    natural_key: list[str] | None = None,
):
    cols = columns(ddl)
    checks = [_is_unique(table, "sk", [sk_col])]
    if natural_key:
        checks.append(_is_unique(table, "natural_key", natural_key))

    @dp.temporary_view(name=f"{table}_checked")
    @dp.expect_all_or_fail({"dq_passed": "_errors IS NULL"})
    def _checked():
        df = build()
        return _engine(df).apply_checks_by_metadata(df, checks)

    @dp.materialized_view(name=table, schema=ddl, comment=comment, cluster_by_auto=True)
    def _dimension():
        return dp.read(f"{table}_checked").select(*cols)