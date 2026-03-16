from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator

import sys
import os

# Make src and config importable inside Airflow workers
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.validate import run_validation
from src.pipeline import read_csv, transform, write_parquet, create_spark_session
from config.settings import INPUT_PATH, OUTPUT_PATH


default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="sales_csv_to_parquet",
    description="Validate → Deduplicate → Filter → Write Parquet",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["pyspark", "etl", "great-expectations"],
) as dag:

    def validate_task():
        """Runs Great Expectations checks. Returns False to short-circuit if failed."""
        passed = run_validation(INPUT_PATH)
        if not passed:
            raise ValueError("Data validation failed — downstream tasks will not run.")
        return passed

    def etl_task():
        """Runs the PySpark ETL: dedupe → filter → write Parquet."""
        spark = create_spark_session("Airflow: CSV to Parquet ETL")
        spark.sparkContext.setLogLevel("WARN")

        df = read_csv(spark, INPUT_PATH)
        print(f"[ETL] Records read: {df.count()}")

        df_out = transform(df)
        print(f"[ETL] Records after transform: {df_out.count()}")

        write_parquet(df_out, OUTPUT_PATH)
        print(f"[ETL] Parquet written to: {OUTPUT_PATH}")

        spark.stop()

    t1_validate = PythonOperator(
        task_id="validate_data",
        python_callable=validate_task,
    )

    t2_etl = PythonOperator(
        task_id="run_etl",
        python_callable=etl_task,
    )

    # Validation must pass before ETL runs
    t1_validate >> t2_etl
