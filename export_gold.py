"""Export Gold tables from DuckDB to Parquet files."""
import os
import duckdb

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
DB_PATH = os.path.join(DATA_DIR, "riskmetric.duckdb")

conn = duckdb.connect(DB_PATH)
for table in ["gold_risk_scores", "gold_user_risk_profiles", "gold_fraud_attribution",
              "gold_model_evaluation", "gold_threshold_calibration"]:
    out = os.path.join(DATA_DIR, f"{table}.parquet").replace("\\", "/")
    conn.execute(f"COPY {table} TO '{out}' (FORMAT PARQUET)")
    size = os.path.getsize(out.replace("/", os.sep)) / 1e6
    print(f"  Exported {table} ({size:.1f} MB)")
conn.close()
print("Done.")
