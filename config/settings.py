import os

# Paths (override via environment variables for prod deployments)
INPUT_PATH  = os.getenv("INPUT_PATH",  "data/input/sales.csv")
OUTPUT_PATH = os.getenv("OUTPUT_PATH", "data/output/sales_filtered")

# Pipeline parameters
SALES_THRESHOLD = int(os.getenv("SALES_THRESHOLD", "1000"))
