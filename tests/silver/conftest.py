import importlib.util

import pytest


def databricks_connect_installed():
    try:
        return importlib.util.find_spec("databricks.connect") is not None
    except ModuleNotFoundError:
        return False


@pytest.fixture(scope="session")
def spark():
    if databricks_connect_installed():
        pytest.skip("needs local Spark: uv run --no-dev --group spark-tests pytest")

    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("formula_1-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()