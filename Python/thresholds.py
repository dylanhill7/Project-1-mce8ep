import duckdb
import logging
import os

# -------------------------------
# Logging setup
# -------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="thresholds.log"
)
logger = logging.getLogger(__name__)

# -------------------------------
# Configuration
# -------------------------------
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

DB_NAME = os.path.join(BASE_DIR, "mlb_analysis.duckdb")


# -------------------------------
# Helper functions
# -------------------------------
def table_exists(con, table_name):
    """
    Check whether a table exists in DuckDB.
    """
    query = """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = ?
    """
    result = con.execute(query, [table_name]).fetchone()[0]
    return result > 0


# -------------------------------
# Main threshold calculation
# -------------------------------
def calculate_thresholds_and_labels():
    con = None

    try:
        # read_only must be False because we are altering tables
        con = duckdb.connect(database=DB_NAME, read_only=False)
        logger.info("Connected to DuckDB database: %s", DB_NAME)
        print(f"Connected to DuckDB database: {DB_NAME}")

        required_tables = ["breakout_master", "regression_master"]
        for table_name in required_tables:
            if not table_exists(con, table_name):
                raise ValueError(
                    f"Required table '{table_name}' does not exist. "
                    "Run your master-table transform script first."
                )

        logger.info("Verified breakout_master and regression_master exist.")
        print("Verified breakout_master and regression_master exist.")

        # -------------------------------
        # Breakout threshold (75th percentile)
        # -------------------------------
        breakout_stats = con.execute("""
            SELECT
                COUNT(*) AS n_rows,
                MIN(ops_diff) AS min_ops_diff,
                MAX(ops_diff) AS max_ops_diff,
                AVG(ops_diff) AS avg_ops_diff,
                MEDIAN(ops_diff) AS median_ops_diff,
                QUANTILE_CONT(ops_diff, 0.75) AS breakout_threshold
            FROM breakout_master
            WHERE ops_diff IS NOT NULL
        """).fetchone()

        (
            breakout_n,
            breakout_min,
            breakout_max,
            breakout_avg,
            breakout_median,
            breakout_threshold
        ) = breakout_stats

        # -------------------------------
        # Regression threshold (25th percentile)
        # -------------------------------
        regression_stats = con.execute("""
            SELECT
                COUNT(*) AS n_rows,
                MIN(ops_diff) AS min_ops_diff,
                MAX(ops_diff) AS max_ops_diff,
                AVG(ops_diff) AS avg_ops_diff,
                MEDIAN(ops_diff) AS median_ops_diff,
                QUANTILE_CONT(ops_diff, 0.25) AS regression_threshold
            FROM regression_master
            WHERE ops_diff IS NOT NULL
        """).fetchone()

        (
            regression_n,
            regression_min,
            regression_max,
            regression_avg,
            regression_median,
            regression_threshold
        ) = regression_stats

        # -------------------------------
        # Add label column to breakout_master
        # -------------------------------
        con.execute("""
            ALTER TABLE breakout_master
            ADD COLUMN IF NOT EXISTS breakout_label INTEGER
        """)

        con.execute("""
            UPDATE breakout_master
            SET breakout_label = CASE
                WHEN ops_diff >= ? THEN 1
                ELSE 0
            END
        """, [breakout_threshold])

        # -------------------------------
        # Add label column to regression_master
        # -------------------------------
        con.execute("""
            ALTER TABLE regression_master
            ADD COLUMN IF NOT EXISTS regression_label INTEGER
        """)

        con.execute("""
            UPDATE regression_master
            SET regression_label = CASE
                WHEN ops_diff <= ? THEN 1
                ELSE 0
            END
        """, [regression_threshold])

        # -------------------------------
        # Count labeled rows
        # -------------------------------
        breakout_label_count = con.execute("""
            SELECT COUNT(*)
            FROM breakout_master
            WHERE breakout_label = 1
        """).fetchone()[0]

        regression_label_count = con.execute("""
            SELECT COUNT(*)
            FROM regression_master
            WHERE regression_label = 1
        """).fetchone()[0]

        # -------------------------------
        # Print results
        # -------------------------------
        print("\nBREAKOUT CANDIDATES (age 20-25)")
        print(f"Rows: {breakout_n}")
        print(f"Min OPS diff: {breakout_min:.3f}")
        print(f"Max OPS diff: {breakout_max:.3f}")
        print(f"Average OPS diff: {breakout_avg:.3f}")
        print(f"Median OPS diff: {breakout_median:.3f}")
        print(f"75th percentile breakout threshold: {breakout_threshold:.3f}")
        print(f"Rows labeled as breakout (1): {breakout_label_count}")

        print("\nREGRESSION CANDIDATES (age 30-35)")
        print(f"Rows: {regression_n}")
        print(f"Min OPS diff: {regression_min:.3f}")
        print(f"Max OPS diff: {regression_max:.3f}")
        print(f"Average OPS diff: {regression_avg:.3f}")
        print(f"Median OPS diff: {regression_median:.3f}")
        print(f"25th percentile regression threshold: {regression_threshold:.3f}")
        print(f"Rows labeled as regression (1): {regression_label_count}")

        logger.info(
            "Calculated thresholds and added labels successfully. "
            "Breakout threshold: %.3f, Regression threshold: %.3f",
            breakout_threshold,
            regression_threshold
        )

    except Exception as e:
        logger.error("An error occurred in calculate_thresholds_and_labels: %s", e)
        print(f"An error occurred: {e}")

    finally:
        if con:
            con.close()
            logger.info("DuckDB connection closed.")
            print("\nDuckDB connection closed.")


if __name__ == "__main__":
    calculate_thresholds_and_labels()