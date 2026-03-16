import great_expectations as gx
from great_expectations.core.expectation_suite import ExpectationSuite
from pyspark.sql import SparkSession


def get_spark():
    return SparkSession.builder \
        .appName("GE Validation") \
        .master("local[*]") \
        .getOrCreate()


def build_suite(context) -> ExpectationSuite:
    suite = context.suites.add(ExpectationSuite(name="sales_suite"))
    return suite


def run_validation(input_path: str = "data/input/sales.csv"):
    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")

    # Load CSV into pandas (GE works natively with pandas)
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(input_path) \
        .toPandas()

    # Set up GE context (in-memory, no project files needed)
    context = gx.get_context(mode="ephemeral")

    # Define expectations
    suite = context.suites.add(ExpectationSuite(name="sales_suite"))

    data_source = context.data_sources.add_pandas("pandas_source")
    data_asset = data_source.add_dataframe_asset("sales_asset")
    batch_def = data_asset.add_batch_definition_whole_dataframe("sales_batch")

    batch = batch_def.get_batch(batch_parameters={"dataframe": df})

    # Run expectations
    results = batch.validate(
        expect=gx.expectations.ExpectColumnToExist(column="sales"),
    )
    results2 = batch.validate(
        expect=gx.expectations.ExpectColumnValuesToBeBetween(
            column="sales", min_value=0
        ),
    )
    results3 = batch.validate(
        expect=gx.expectations.ExpectColumnValuesToNotBeNull(column="sales"),
    )
    results4 = batch.validate(
        expect=gx.expectations.ExpectColumnValuesToNotBeNull(column="customer"),
    )
    results5 = batch.validate(
        expect=gx.expectations.ExpectTableRowCountToBeBetween(min_value=1),
    )

    all_results = [results, results2, results3, results4, results5]
    expectations = [
        "sales column exists",
        "sales values >= 0",
        "sales has no nulls",
        "customer has no nulls",
        "table has at least 1 row",
    ]

    print("\n====== Great Expectations Validation Report ======")
    all_passed = True
    for label, result in zip(expectations, all_results):
        status = "PASS" if result.success else "FAIL"
        if not result.success:
            all_passed = False
        print(f"  [{status}] {label}")

    print("==================================================")
    print(f"  Overall: {'ALL PASSED' if all_passed else 'SOME CHECKS FAILED'}")
    print("==================================================\n")

    spark.stop()
    return all_passed


if __name__ == "__main__":
    run_validation()
