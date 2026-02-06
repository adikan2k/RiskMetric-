"""
RiskMetric — Full Pipeline Runner
Orchestrates the entire Bronze → Silver → Gold pipeline:
  1. Generate synthetic transactions (Bronze layer)
  2. Run dbt models (Silver + Gold layers)
  3. Export Gold tables to Parquet for dashboard consumption
"""

import os
import sys
import subprocess
import time
import duckdb

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
DBT_DIR = os.path.join(PROJECT_ROOT, "riskmetric")
DB_PATH = os.path.join(DATA_DIR, "riskmetric.duckdb")


def step_banner(step: int, title: str):
    print(f"\n{'='*60}")
    print(f"  STEP {step}: {title}")
    print(f"{'='*60}\n")


def run_generator():
    """Step 1: Generate Bronze-layer data."""
    step_banner(1, "BRONZE — Generating Synthetic Transactions")
    gen_script = os.path.join(PROJECT_ROOT, "generator.py")
    result = subprocess.run(
        [sys.executable, gen_script],
        cwd=PROJECT_ROOT,
        capture_output=False,
    )
    if result.returncode != 0:
        print("ERROR: Generator failed!")
        sys.exit(1)
    print("Bronze layer complete.")


def run_dbt():
    """Step 2: Run dbt models (Silver + Gold layers)."""
    step_banner(2, "SILVER + GOLD — Running dbt Models")

    # Resolve absolute path for parquet files
    raw_txn_path = os.path.join(DATA_DIR, "raw_transactions.parquet").replace("\\", "/")
    user_prof_path = os.path.join(DATA_DIR, "user_profiles.parquet").replace("\\", "/")

    dbt_vars = (
        f'{{"raw_transactions_path": "{raw_txn_path}", '
        f'"user_profiles_path": "{user_prof_path}"}}'
    )

    # dbt run
    print("Running dbt models...")
    result = subprocess.run(
        [
            "dbt", "run",
            "--profiles-dir", DBT_DIR,
            "--project-dir", DBT_DIR,
            "--vars", dbt_vars,
        ],
        cwd=DBT_DIR,
        capture_output=False,
    )
    if result.returncode != 0:
        print("ERROR: dbt run failed!")
        sys.exit(1)

    # dbt test
    print("\nRunning dbt tests...")
    result = subprocess.run(
        [
            "dbt", "test",
            "--profiles-dir", DBT_DIR,
            "--project-dir", DBT_DIR,
            "--vars", dbt_vars,
        ],
        cwd=DBT_DIR,
        capture_output=False,
    )
    if result.returncode != 0:
        print("WARNING: Some dbt tests failed. Check output above.")
    else:
        print("All dbt tests passed.")


def export_gold_tables():
    """Step 3: Export Gold tables to Parquet for external consumption."""
    step_banner(3, "EXPORT — Gold Tables to Parquet")

    conn = duckdb.connect(DB_PATH)
    gold_tables = ["gold_risk_scores", "gold_user_risk_profiles", "gold_fraud_attribution",
                   "gold_model_evaluation", "gold_threshold_calibration"]

    for table in gold_tables:
        out_path = os.path.join(DATA_DIR, f"{table}.parquet")
        try:
            conn.execute(f"COPY {table} TO '{out_path}' (FORMAT PARQUET)")
            size_mb = os.path.getsize(out_path) / 1e6
            print(f"  Exported {table} -> {out_path} ({size_mb:.1f} MB)")
        except Exception as e:
            print(f"  WARNING: Could not export {table}: {e}")

    conn.close()


def generate_dbt_docs():
    """Step 4: Generate dbt documentation and data lineage graph."""
    step_banner(4, "DOCS — Generating dbt Documentation")

    raw_txn_path = os.path.join(DATA_DIR, "raw_transactions.parquet").replace("\\", "/")
    user_prof_path = os.path.join(DATA_DIR, "user_profiles.parquet").replace("\\", "/")
    dbt_vars = (
        f'{{"raw_transactions_path": "{raw_txn_path}", '
        f'"user_profiles_path": "{user_prof_path}"}}'
    )

    result = subprocess.run(
        [
            "dbt", "docs", "generate",
            "--profiles-dir", DBT_DIR,
            "--project-dir", DBT_DIR,
            "--vars", dbt_vars,
        ],
        cwd=DBT_DIR,
        capture_output=False,
    )
    if result.returncode != 0:
        print("WARNING: dbt docs generation failed.")
    else:
        print("dbt docs generated -> riskmetric/target/")
        print("  View with: dbt docs serve --profiles-dir riskmetric --project-dir riskmetric")


def print_summary():
    """Print pipeline summary statistics."""
    step_banner(5, "PIPELINE SUMMARY")

    conn = duckdb.connect(DB_PATH, read_only=True)

    total = conn.execute("SELECT COUNT(*) FROM gold_risk_scores").fetchone()[0]
    flagged = conn.execute("SELECT COUNT(*) FROM gold_risk_scores WHERE detected_fraud = TRUE").fetchone()[0]
    critical = conn.execute("SELECT COUNT(*) FROM gold_risk_scores WHERE risk_tier = 'CRITICAL'").fetchone()[0]
    high = conn.execute("SELECT COUNT(*) FROM gold_risk_scores WHERE risk_tier = 'HIGH'").fetchone()[0]

    it_flags = conn.execute(
        "SELECT COUNT(*) FROM gold_risk_scores WHERE flag_impossible_travel = TRUE"
    ).fetchone()[0]
    vs_flags = conn.execute(
        "SELECT COUNT(*) FROM gold_risk_scores WHERE flag_velocity_spike = TRUE"
    ).fetchone()[0]
    bd_flags = conn.execute(
        "SELECT COUNT(*) FROM gold_risk_scores WHERE flag_behavioral_drift = TRUE"
    ).fetchone()[0]

    users_at_risk = conn.execute(
        "SELECT COUNT(*) FROM gold_user_risk_profiles WHERE user_risk_tier IN ('CRITICAL', 'HIGH')"
    ).fetchone()[0]

    # Model evaluation metrics
    eval_rows = conn.execute("SELECT * FROM gold_model_evaluation ORDER BY archetype").fetchall()

    conn.close()

    print(f"  Total Transactions      : {total:>12,}")
    print(f"  Flagged as Fraud        : {flagged:>12,}")
    print(f"  Critical Alerts         : {critical:>12,}")
    print(f"  High Risk Alerts        : {high:>12,}")
    print(f"  Impossible Travel Flags : {it_flags:>12,}")
    print(f"  Velocity Spike Flags    : {vs_flags:>12,}")
    print(f"  Behavioral Drift Flags  : {bd_flags:>12,}")
    print(f"  Users at Risk (C+H)     : {users_at_risk:>12,}")
    print(f"  Fraud Detection Rate    : {flagged/total*100:>11.2f}%")
    print()
    print("  ── Model Evaluation ──────────────────────────────────")
    print(f"  {'Archetype':<22} {'Precision':>10} {'Recall':>10} {'F1':>10} {'FPR':>10}")
    print(f"  {'─'*22} {'─'*10} {'─'*10} {'─'*10} {'─'*10}")
    for row in eval_rows:
        arch = row[0]
        prec = row[6]
        rec = row[7]
        f1 = row[8]
        fpr = row[10]
        print(f"  {arch:<22} {prec:>10.4f} {rec:>10.4f} {f1:>10.4f} {fpr:>10.6f}")
    print()
    print("  Launch dashboard:  streamlit run dashboard/app.py")
    print("  View dbt docs:     dbt docs serve --profiles-dir riskmetric --project-dir riskmetric")
    print()


def main():
    start = time.time()
    print("\n" + "=" * 60)
    print("  RiskMetric — Full Pipeline Execution")
    print("=" * 60)

    run_generator()
    run_dbt()
    export_gold_tables()
    generate_dbt_docs()
    print_summary()

    elapsed = time.time() - start
    print(f"  Total pipeline time: {elapsed:.1f}s")
    print("=" * 60)


if __name__ == "__main__":
    main()
