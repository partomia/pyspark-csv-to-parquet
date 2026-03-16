from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from config.settings import INPUT_PATH, OUTPUT_PATH, SALES_THRESHOLD


def create_spark_session(app_name: str) -> SparkSession:
    return SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .getOrCreate()


def read_csv(spark: SparkSession, path: str):
    return spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(path)


def transform(df):
    df_deduped = df.dropDuplicates()
    df_filtered = df_deduped.filter(col("sales") > SALES_THRESHOLD)
    return df_filtered


def write_parquet(df, path: str):
    df.write \
        .mode("overwrite") \
        .parquet(path)


def main():
    spark = create_spark_session("CSV to Parquet ETL")
    spark.sparkContext.setLogLevel("WARN")

    print(f"[INFO] Reading CSV from: {INPUT_PATH}")
    df = read_csv(spark, INPUT_PATH)
    print(f"[INFO] Records read       : {df.count()}")

    df_out = transform(df)
    print(f"[INFO] Records after transform: {df_out.count()}")

    write_parquet(df_out, OUTPUT_PATH)
    print(f"[INFO] Parquet written to : {OUTPUT_PATH}")

    spark.stop()


if __name__ == "__main__":
    main()
