import pytest
from pyspark.sql import SparkSession
from src.pipeline import transform


@pytest.fixture(scope="session")
def spark():
    session = SparkSession.builder \
        .appName("test-csv-to-parquet") \
        .master("local[2]") \
        .getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def test_deduplication(spark):
    data = [
        (1, "Alice", 1500),
        (1, "Alice", 1500),  # duplicate
        (2, "Bob",   800),
    ]
    df = spark.createDataFrame(data, ["id", "customer", "sales"])
    result = transform(df)
    assert result.count() == 1  # Bob filtered out, Alice deduped to 1


def test_sales_filter(spark):
    data = [
        (1, "Alice", 500),
        (2, "Bob",   1001),
        (3, "Carol", 1000),  # boundary — NOT included (strictly >)
    ]
    df = spark.createDataFrame(data, ["id", "customer", "sales"])
    result = transform(df)
    assert result.count() == 1
    assert result.first()["customer"] == "Bob"
