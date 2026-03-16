# PySpark CSV to Parquet ETL

A PySpark pipeline that validates, deduplicates, filters, and converts CSV data to Parquet format. Includes data quality checks via Great Expectations and Airflow DAG orchestration.

---

## What it does

1. **Validates** the input CSV using Great Expectations (nulls, column existence, value ranges)
2. **Deduplicates** rows based on `customer`, `region`, and `sales`
3. **Filters** records where `sales > 1000`
4. **Writes** the result as Parquet

---

## Project Structure

```
pyspark-csv-to-parquet/
├── config/
│   └── settings.py          # Input/output paths and pipeline config
├── dags/
│   └── sales_pipeline_dag.py  # Airflow DAG
├── data/
│   ├── input/sales.csv      # Sample input data
│   └── output/              # Parquet output (auto-generated)
├── src/
│   ├── pipeline.py          # Main ETL logic
│   └── validate.py          # Great Expectations validation
├── tests/
│   └── test_pipeline.py     # Unit tests
└── requirements.txt
```

---

## Requirements

- Python 3.9+
- Java 8 or 11 (required by Spark)

---

## Setup

```bash
git clone https://github.com/partomia/pyspark-csv-to-parquet.git
cd pyspark-csv-to-parquet
pip install -r requirements.txt
```

---

## Running the Pipeline

```bash
PYTHONPATH=. python3 src/pipeline.py
```

This will:
- Run Great Expectations validation first — aborts if any check fails
- Run the ETL and write Parquet to `data/output/sales_filtered/`

### Custom paths

Override paths via environment variables without changing any code:

```bash
INPUT_PATH=/your/data/file.csv \
OUTPUT_PATH=/your/output/folder \
SALES_THRESHOLD=2000 \
PYTHONPATH=. python3 src/pipeline.py
```

---

## Input CSV Format

The input CSV must have a header row with at least these columns:

| Column     | Type   | Description          |
|------------|--------|----------------------|
| `order_id` | int    | Unique order ID      |
| `customer` | string | Customer name        |
| `region`   | string | Sales region         |
| `sales`    | int    | Sales amount         |

Example:

```csv
order_id,customer,region,sales
1,Alice,West,1500
2,Bob,East,800
3,Carol,North,2200
```

---

## Running Validation Only

```bash
PYTHONPATH=. python3 src/validate.py
```

Checks performed:
- `sales` column exists
- `sales` values are >= 0
- `sales` has no nulls
- `customer` has no nulls
- Table has at least 1 row

---

## Running Tests

```bash
pytest tests/
```

---

## Airflow Orchestration

### Setup

```bash
pip install apache-airflow

export AIRFLOW_HOME=$(pwd)
airflow db init
airflow users create \
  --username admin --password admin \
  --firstname Admin --lastname User \
  --role Admin --email admin@example.com
```

### Start Airflow

```bash
airflow webserver --port 8080 &
airflow scheduler &
```

Open **http://localhost:8080**, log in with `admin / admin`, and trigger the `sales_csv_to_parquet` DAG.

### DAG Flow

```
validate_data  ──►  run_etl
```

- Runs daily (`@daily`)
- If validation fails, the ETL task is skipped automatically
- Retries once on failure with a 5-minute delay

---

## Configuration

All settings are in `config/settings.py` and can be overridden with environment variables:

| Variable           | Default                        | Description              |
|--------------------|--------------------------------|--------------------------|
| `INPUT_PATH`       | `data/input/sales.csv`         | Path to input CSV        |
| `OUTPUT_PATH`      | `data/output/sales_filtered`   | Path for Parquet output  |
| `SALES_THRESHOLD`  | `1000`                         | Minimum sales to include |
