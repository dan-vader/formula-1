from collections.abc import Callable

from pyspark import pipelines as dp
from pyspark.sql import DataFrame

from formula_1.silver import config
from formula_1.silver.transforms import dq

TABLES = {}


def _same(df: DataFrame) -> DataFrame:
    return df


def register(table,
            parsed: Callable[[], DataFrame],
            keys: list[str], ddl: str, comment: str,
            filtered: Callable[[DataFrame], DataFrame] = _same,
            deduped: Callable[[DataFrame], DataFrame] = _same,
            enriched: Callable[[DataFrame], DataFrame] = _same,
            ref_dfs=None):
    short = table.removeprefix("silver_")
    TABLES[table] = short
    cols = config.field_names(ddl)

    @dp.temporary_view(name=f"{table}_parsed")
    def _parsed():
        return parsed()

    @dp.temporary_view(name=f"{table}_filtered")
    def _filtered():
        return filtered(dp.read(f"{table}_parsed"))

    @dp.temporary_view(name=f"{table}_deduped")
    def _deduped():
        return deduped(dp.read(f"{table}_filtered"))

    # DQX: adds _errors / _warnings
    @dp.temporary_view(name=f"{table}_checked")
    def _checked():
        df = enriched(dp.read(f"{table}_deduped"))
        return dq.apply(df, table, ref_dfs() if ref_dfs else None)

    @dp.materialized_view(name=table, schema=ddl, comment=comment)
    @dp.expect_all_or_fail({f"{k}_not_null": f"{k} IS NOT NULL" for k in keys})
    def _valid():
        return dq.valid(dp.read(f"{table}_checked")).select(*cols)

    @dp.materialized_view(name=f"silver_quarantine_{short}")
    def _quarantine():
        return dq.quarantine(dp.read(f"{table}_checked"), table, keys)
