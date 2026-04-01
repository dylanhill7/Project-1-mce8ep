
# load.py

import duckdb
import logging
import os
from pathlib import Path

# -------------------------------
# Logging setup
# -------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="load.log"
)
logger = logging.getLogger(__name__)

# -------------------------------
# Configuration
# -------------------------------
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

DB_NAME = os.path.join(BASE_DIR, "mlb_analysis.duckdb")

# Paths to data folders
AGE_20_25_FOLDER = os.path.join(BASE_DIR, "Data", "20-25")
AGE_30_35_FOLDER = os.path.join(BASE_DIR, "Data", "30-35")

# file names, excluding 2020 in range because of shortened covid 19 season
AGE_20_25_FILES = [
    "20-25(1985-2019).csv",
    "20-25(2021-2025).csv"
]

AGE_30_35_FILES = [
    "30-35(1985-2019).csv",
    "30-35(2021-2025).csv"
]

# -------------------------------
# Helper functions
# -------------------------------

# check whether a table already exists in DuckDB to avoid unnecessary reloads
def table_exists(con, table_name):
    query = """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = ?
    """
    result = con.execute(query, [table_name]).fetchone()[0]
    return result > 0

# raise an error if a required file doesn't exist
def validate_file_exists(file_path):
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Required file not found: {file_path}")

# loads all CSVs for an age group into duckDB, both 1985-2019 and 2021-2025
def load_age_group_table(con, table_name, folder_path, file_names):
    if table_exists(con, table_name):
        logger.info("Table '%s' already exists. Skipping load.", table_name)
        print(f"'{table_name}' already exists, nothing to load.")
        return

    first_file_loaded = False

    for file_name in file_names:
        file_path = os.path.join(folder_path, file_name)

        try:
            validate_file_exists(file_path)

            if not first_file_loaded:
                # Create table from first file
                con.execute(f"""
                    CREATE TABLE {table_name} AS
                    SELECT *
                    FROM read_csv_auto(?, HEADER=TRUE)
                """, [file_path])

                logger.info(
                    "Created table '%s' from file %s",
                    table_name, file_path
                )
                print(f"Created '{table_name}' with {file_name}")
                first_file_loaded = True

            else:
                # Insert remaining files
                con.execute(f"""
                    INSERT INTO {table_name}
                    SELECT *
                    FROM read_csv_auto(?, HEADER=TRUE)
                """, [file_path])

                logger.info(
                    "Inserted file %s into '%s'",
                    file_path, table_name
                )
                print(f"Loaded {file_name} into '{table_name}'")

        except FileNotFoundError as fnf_error:
            logger.error("Missing file for table '%s': %s", table_name, fnf_error)
            print(f"Missing file: {fnf_error}")
            raise

        except Exception as e:
            logger.error("Failed while loading %s into %s: %s", file_path, table_name, e)
            print(f"An error occurred while loading {file_name}: {e}")
            raise

# print/log basic summary info for a table
def print_table_summary(con, table_name):
    try:
        row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        logger.info("Total rows in '%s': %s", table_name, row_count)
        print(f"Total rows in '{table_name}': {row_count}")

        summary = con.execute(f"""
            SELECT
                MIN(Season) AS min_season,
                MAX(Season) AS max_season,
                COUNT(DISTINCT Name) AS unique_players
            FROM {table_name}
        """).fetchone()

        min_season, max_season, unique_players = summary

        summary_str = (
            f"Summary for '{table_name}':\n"
            f"  Seasons covered: {min_season} to {max_season}\n"
            f"  Unique players: {unique_players}\n"
        )

        logger.info(summary_str)
        print(summary_str)

    except Exception as e:
        logger.error("Could not summarize table '%s': %s", table_name, e)
        print(f"Could not summarize '{table_name}': {e}")


# -------------------------------
# Main loader
# -------------------------------
def load_mlb_data():
    con = None

    try:
        con = duckdb.connect(database=DB_NAME, read_only=False)
        logger.info("Connected to DuckDB database: %s", DB_NAME)
        print(f"Connected to DuckDB database: {DB_NAME}")

        age_20_25_folder = str(Path(AGE_20_25_FOLDER))
        age_30_35_folder = str(Path(AGE_30_35_FOLDER))

        load_age_group_table(
            con=con,
            table_name="hitters_20_25",
            folder_path=age_20_25_folder,
            file_names=AGE_20_25_FILES
        )

        load_age_group_table(
            con=con,
            table_name="hitters_30_35",
            folder_path=age_30_35_folder,
            file_names=AGE_30_35_FILES
        )

        print_table_summary(con, "hitters_20_25")
        print_table_summary(con, "hitters_30_35")

        logger.info("Finished loading all MLB Fangraphs data.")
        print("Finished loading all MLB Fangraphs data.")

    except Exception as e:
        logger.error("An error occurred in load_mlb_data: %s", e)
        print(f"An error occurred: {e}")

    finally:
        if con:
            con.close()
            logger.info("DuckDB connection closed.")
            print("DuckDB connection closed.")


if __name__ == "__main__":
    load_mlb_data()

# transform1.py

import duckdb
import logging
import os

# -------------------------------
# Logging setup
# -------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="transform.log"
)
logger = logging.getLogger(__name__)

# -------------------------------
# Configuration
# -------------------------------
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

DB_NAME = os.path.join(BASE_DIR, "mlb_analysis.duckdb")
CSV_OUTPUT_DIR = os.path.join(BASE_DIR, "Data", "relational_tables")

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


def log_row_count(con, table_name):
    """
    Log and print the row count for a table.
    """
    count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    logger.info("Total rows in '%s': %s", table_name, count)
    print(f"Total rows in '{table_name}': {count}")


def export_table_to_csv(con, table_name, output_dir):
    """
    Export a DuckDB table to CSV.
    """
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"{table_name}.csv")

    con.execute(f"""
        COPY {table_name}
        TO '{output_path}'
        (HEADER, DELIMITER ',')
    """)

    logger.info("Exported '%s' to %s", table_name, output_path)
    print(f"Exported '{table_name}' to {output_path}")


# -------------------------------
# Main transformation pipeline
# -------------------------------
def transform_data():
    con = None

    try:
        # Connect to DuckDB
        con = duckdb.connect(database=DB_NAME, read_only=False)
        logger.info("Connected to DuckDB database: %s", DB_NAME)
        print(f"Connected to DuckDB database: {DB_NAME}")

        # Check that the two staging tables exist
        required_tables = ["hitters_20_25", "hitters_30_35"]
        for table_name in required_tables:
            if not table_exists(con, table_name):
                raise ValueError(
                    f"Required staging table '{table_name}' does not exist. "
                    "Run load.py first."
                )

        logger.info("Verified staging tables exist.")
        print("Verified staging tables exist.")

        # -------------------------------
        # Create seasons lookup table
        # -------------------------------
        con.execute("DROP TABLE IF EXISTS seasons")

        con.execute("""
            CREATE TABLE seasons AS
            WITH distinct_seasons AS (
                SELECT DISTINCT Season AS year
                FROM hitters_20_25
                UNION
                SELECT DISTINCT Season AS year
                FROM hitters_30_35
            )
            SELECT
                year - 1984 AS season_id,
                year
            FROM distinct_seasons
            ORDER BY year
        """)

        logger.info("Created 'seasons' table.")
        print("Created 'seasons' table.")
        log_row_count(con, "seasons")

        # -------------------------------
        # Create players lookup table
        # -------------------------------
        con.execute("DROP TABLE IF EXISTS players")

        con.execute("""
            CREATE TABLE players AS
            WITH distinct_players AS (
                SELECT DISTINCT TRIM(Name) AS player_name
                FROM hitters_20_25
                UNION
                SELECT DISTINCT TRIM(Name) AS player_name
                FROM hitters_30_35
            )
            SELECT
                ROW_NUMBER() OVER (ORDER BY player_name) AS player_id,
                player_name
            FROM distinct_players
            ORDER BY player_name
        """)

        logger.info("Created 'players' table.")
        print("Created 'players' table.")
        log_row_count(con, "players")

        # -------------------------------
        # Add player_id and season_id to staging tables
        # -------------------------------
        con.execute("""
            ALTER TABLE hitters_20_25
            ADD COLUMN IF NOT EXISTS player_id INTEGER
        """)
        con.execute("""
            ALTER TABLE hitters_20_25
            ADD COLUMN IF NOT EXISTS season_id INTEGER
        """)

        con.execute("""
            UPDATE hitters_20_25 AS h
            SET player_id = p.player_id
            FROM players AS p
            WHERE TRIM(h.Name) = p.player_name
        """)

        con.execute("""
            UPDATE hitters_20_25 AS h
            SET season_id = s.season_id
            FROM seasons AS s
            WHERE h.Season = s.year
        """)

        logger.info("Added and populated player_id and season_id in 'hitters_20_25'.")
        print("Added and populated player_id and season_id in 'hitters_20_25'.")

        con.execute("""
            ALTER TABLE hitters_30_35
            ADD COLUMN IF NOT EXISTS player_id INTEGER
        """)
        con.execute("""
            ALTER TABLE hitters_30_35
            ADD COLUMN IF NOT EXISTS season_id INTEGER
        """)

        con.execute("""
            UPDATE hitters_30_35 AS h
            SET player_id = p.player_id
            FROM players AS p
            WHERE TRIM(h.Name) = p.player_name
        """)

        con.execute("""
            UPDATE hitters_30_35 AS h
            SET season_id = s.season_id
            FROM seasons AS s
            WHERE h.Season = s.year
        """)

        logger.info("Added and populated player_id and season_id in 'hitters_30_35'.")
        print("Added and populated player_id and season_id in 'hitters_30_35'.")

        # -------------------------------
        # Create breakout mainstream batting stats table (age 20-25)
        # -------------------------------
        con.execute("DROP TABLE IF EXISTS mainstream_batting_stats_breakout")

        con.execute("""
            CREATE TABLE mainstream_batting_stats_breakout AS
            SELECT
                player_id,
                season_id,
                G,
                PA,
                HR,
                R,
                RBI,
                SB,
                AVG,
                OBP,
                SLG
            FROM hitters_20_25
        """)

        con.execute("""
            ALTER TABLE mainstream_batting_stats_breakout
            ADD COLUMN IF NOT EXISTS OPS DOUBLE
        """)

        con.execute("""
            UPDATE mainstream_batting_stats_breakout
            SET OPS = CAST(OBP AS DOUBLE) + CAST(SLG AS DOUBLE)
        """)

        logger.info("Created 'mainstream_batting_stats_breakout' table.")
        print("Created 'mainstream_batting_stats_breakout' table.")
        log_row_count(con, "mainstream_batting_stats_breakout")

        # -------------------------------
        # Create breakout advanced batting stats table (age 20-25)
        # -------------------------------
        con.execute("DROP TABLE IF EXISTS advanced_batting_stats_breakout")

        con.execute("""
            CREATE TABLE advanced_batting_stats_breakout AS
            SELECT
                player_id,
                season_id,
                "BB%" AS "BB%",
                "K%" AS "K%",
                ISO,
                BABIP,
                wOBA,
                xwOBA,
                "wRC+" AS "wRC+",
                BsR,
                Off,
                Def,
                WAR
            FROM hitters_20_25
        """)

        logger.info("Created 'advanced_batting_stats_breakout' table.")
        print("Created 'advanced_batting_stats_breakout' table.")
        log_row_count(con, "advanced_batting_stats_breakout")

        # -------------------------------
        # Create regression mainstream batting stats table (age 30-35)
        # -------------------------------
        con.execute("DROP TABLE IF EXISTS mainstream_batting_stats_regression")

        con.execute("""
            CREATE TABLE mainstream_batting_stats_regression AS
            SELECT
                player_id,
                season_id,
                G,
                PA,
                HR,
                R,
                RBI,
                SB,
                AVG,
                OBP,
                SLG
            FROM hitters_30_35
        """)

        con.execute("""
            ALTER TABLE mainstream_batting_stats_regression
            ADD COLUMN IF NOT EXISTS OPS DOUBLE
        """)

        con.execute("""
            UPDATE mainstream_batting_stats_regression
            SET OPS = CAST(OBP AS DOUBLE) + CAST(SLG AS DOUBLE)
        """)

        logger.info("Created 'mainstream_batting_stats_regression' table.")
        print("Created 'mainstream_batting_stats_regression' table.")
        log_row_count(con, "mainstream_batting_stats_regression")

        # -------------------------------
        # Create regression advanced batting stats table (age 30-35)
        # -------------------------------
        con.execute("DROP TABLE IF EXISTS advanced_batting_stats_regression")

        con.execute("""
            CREATE TABLE advanced_batting_stats_regression AS
            SELECT
                player_id,
                season_id,
                "BB%" AS "BB%",
                "K%" AS "K%",
                ISO,
                BABIP,
                wOBA,
                xwOBA,
                "wRC+" AS "wRC+",
                BsR,
                Off,
                Def,
                WAR
            FROM hitters_30_35
        """)

        logger.info("Created 'advanced_batting_stats_regression' table.")
        print("Created 'advanced_batting_stats_regression' table.")
        log_row_count(con, "advanced_batting_stats_regression")

        # -------------------------------
        # Export the 6 relational tables to CSV
        # -------------------------------
        relational_tables = [
            "players",
            "seasons",
            "mainstream_batting_stats_breakout",
            "advanced_batting_stats_breakout",
            "mainstream_batting_stats_regression",
            "advanced_batting_stats_regression"
        ]

        for table_name in relational_tables:
            export_table_to_csv(con, table_name, CSV_OUTPUT_DIR)

        # -------------------------------
        # Final success message
        # -------------------------------
        logger.info("Finished transforming staging tables into 6 relational tables and exporting CSVs.")
        print("Finished transforming staging tables into 6 relational tables and exporting CSVs.")

    except Exception as e:
        logger.error("An error occurred in transform_data: %s", e)
        print(f"An error occurred: {e}")

    finally:
        if con:
            con.close()
            logger.info("DuckDB connection closed.")
            print("DuckDB connection closed.")


if __name__ == "__main__":
    transform_data()

# transform2.py

import duckdb
import logging
import os

# -------------------------------
# Logging setup
# -------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="transform2.log"
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


def log_row_count(con, table_name):
    """
    Log and print the row count for a table.
    """
    count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    logger.info("Total rows in '%s': %s", table_name, count)
    print(f"Total rows in '{table_name}': {count}")


def preview_table(con, table_name, limit=10):
    """
    Print a preview of a table.
    """
    print(f"\nPreview of '{table_name}' (first {limit} rows):")
    print(con.execute(f"SELECT * FROM {table_name} LIMIT {limit}").fetchdf())


# -------------------------------
# Main transformation pipeline
# -------------------------------
def build_master_tables():
    con = None

    try:
        # Connect to DuckDB
        con = duckdb.connect(database=DB_NAME, read_only=False)
        logger.info("Connected to DuckDB database: %s", DB_NAME)
        print(f"Connected to DuckDB database: {DB_NAME}")

        # Check that required tables exist
        required_tables = [
            "players",
            "seasons",
            "mainstream_batting_stats_breakout",
            "advanced_batting_stats_breakout",
            "mainstream_batting_stats_regression",
            "advanced_batting_stats_regression"
        ]

        for table_name in required_tables:
            if not table_exists(con, table_name):
                raise ValueError(
                    f"Required table '{table_name}' does not exist. "
                    "Run transform1.py first."
                )

        logger.info("Verified all required relational tables exist.")
        print("Verified all required relational tables exist.")

        # -------------------------------
        # Create breakout master table
        # -------------------------------
        con.execute("DROP TABLE IF EXISTS breakout_master")

        con.execute("""
            CREATE TABLE breakout_master AS
            SELECT
                p.player_name,
                s2.year AS later_season,
                m2.player_id,
                m2.season_id,
                CAST(m1.OPS AS DOUBLE) AS ops1,
                CAST(m2.OPS AS DOUBLE) AS ops2,
                CAST(m2.OPS AS DOUBLE) - CAST(m1.OPS AS DOUBLE) AS ops_diff,

                a1."BB%" AS "BB%",
                a1."K%" AS "K%",
                a1.ISO,
                a1.BABIP,
                a1.wOBA,
                a1.xwOBA,
                a1."wRC+" AS "wRC+",
                a1.BsR,
                a1.Off,
                a1.Def,
                a1.WAR

            FROM mainstream_batting_stats_breakout AS m2

            JOIN mainstream_batting_stats_breakout AS m1
                ON m2.player_id = m1.player_id
               AND m2.season_id = m1.season_id + 1

            JOIN advanced_batting_stats_breakout AS a1
                ON m1.player_id = a1.player_id
               AND m1.season_id = a1.season_id

            JOIN players AS p
                ON m2.player_id = p.player_id

            JOIN seasons AS s2
                ON m2.season_id = s2.season_id
        """)

        logger.info("Created 'breakout_master' table.")
        print("Created 'breakout_master' table.")
        log_row_count(con, "breakout_master")

        # -------------------------------
        # Create regression master table
        # -------------------------------
        con.execute("DROP TABLE IF EXISTS regression_master")

        con.execute("""
            CREATE TABLE regression_master AS
            SELECT
                p.player_name,
                s2.year AS later_season,
                m2.player_id,
                m2.season_id,
                CAST(m1.OPS AS DOUBLE) AS ops1,
                CAST(m2.OPS AS DOUBLE) AS ops2,
                CAST(m2.OPS AS DOUBLE) - CAST(m1.OPS AS DOUBLE) AS ops_diff,

                a1."BB%" AS "BB%",
                a1."K%" AS "K%",
                a1.ISO,
                a1.BABIP,
                a1.wOBA,
                a1.xwOBA,
                a1."wRC+" AS "wRC+",
                a1.BsR,
                a1.Off,
                a1.Def,
                a1.WAR

            FROM mainstream_batting_stats_regression AS m2

            JOIN mainstream_batting_stats_regression AS m1
                ON m2.player_id = m1.player_id
               AND m2.season_id = m1.season_id + 1

            JOIN advanced_batting_stats_regression AS a1
                ON m1.player_id = a1.player_id
               AND m1.season_id = a1.season_id

            JOIN players AS p
                ON m2.player_id = p.player_id

            JOIN seasons AS s2
                ON m2.season_id = s2.season_id
        """)

        logger.info("Created 'regression_master' table.")
        print("Created 'regression_master' table.")
        log_row_count(con, "regression_master")

        # -------------------------------
        # Preview both master tables
        # -------------------------------
        preview_table(con, "breakout_master", limit=10)
        preview_table(con, "regression_master", limit=10)

        logger.info("Finished building both master tables.")
        print("Finished building both master tables.")

    except Exception as e:
        logger.error("An error occurred in build_master_tables: %s", e)
        print(f"An error occurred: {e}")

    finally:
        if con:
            con.close()
            logger.info("DuckDB connection closed.")
            print("DuckDB connection closed.")


if __name__ == "__main__":
    build_master_tables()

# thresholds.py

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

# analysis.py

import duckdb
import logging
import pandas as pd
import os
import joblib

from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split, GridSearchCV, StratifiedKFold
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix

# -------------------------------
# Logging setup
# -------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="analysis.log"
)
logger = logging.getLogger(__name__)

# -------------------------------
# Configuration
# -------------------------------
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

DB_NAME = os.path.join(BASE_DIR, "mlb_analysis.duckdb")
MODEL_DIR = os.path.join(BASE_DIR, "models")

# xwOBA removed
FEATURE_COLUMNS = [
    "BB%",
    "K%",
    "ISO",
    "BABIP",
    "wOBA",
    "wRC+",
    "BsR",
    "Off",
    "Def",
    "WAR"
]

BREAKOUT_TABLE = "breakout_master"
REGRESSION_TABLE = "regression_master"

BREAKOUT_TARGET = "breakout_label"
REGRESSION_TARGET = "regression_label"

RANDOM_STATE = 42


# -------------------------------
# Helper functions
# -------------------------------
def table_exists(con, table_name):
    query = """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = ?
    """
    result = con.execute(query, [table_name]).fetchone()[0]
    return result > 0


def load_table(con, table_name):
    df = con.execute(f"SELECT * FROM {table_name}").fetchdf()
    logger.info("Loaded table '%s' with %d rows.", table_name, len(df))
    return df


def preprocess_dataframe(df, feature_cols, target_col, dataset_name):
    """
    Drop xwOBA, convert to numeric, and impute missing values with medians.
    Returns:
        working_df: cleaned dataframe
        medians: feature medians used for imputation
    """
    if "xwOBA" in df.columns:
        df = df.drop(columns=["xwOBA"])
        logger.info("Dropped xwOBA column from %s.", dataset_name)

    working_df = df[feature_cols + [target_col]].copy()

    for col in feature_cols:
        working_df[col] = pd.to_numeric(working_df[col], errors="coerce")

    working_df[target_col] = pd.to_numeric(working_df[target_col], errors="coerce")

    print(f"\nMissing values BEFORE imputation for {dataset_name}:")
    print(working_df.isnull().sum())

    logger.info(
        "Missing values before imputation for %s:\n%s",
        dataset_name,
        working_df.isnull().sum().to_string()
    )

    medians = working_df[feature_cols].median()

    working_df[feature_cols] = working_df[feature_cols].fillna(medians)

    working_df = working_df.dropna(subset=[target_col])

    print(f"\nMissing values AFTER imputation for {dataset_name}:")
    print(working_df.isnull().sum())

    logger.info(
        "Missing values after imputation for %s:\n%s",
        dataset_name,
        working_df.isnull().sum().to_string()
    )

    return working_df, medians


def save_model_artifact(
    model,
    feature_cols,
    feature_medians,
    best_params,
    best_cv_score,
    test_accuracy,
    output_path
):
    artifact = {
        "model": model,
        "feature_columns": feature_cols,
        "feature_medians": feature_medians.to_dict(),
        "best_params": best_params,
        "best_cv_score": best_cv_score,
        "test_accuracy": test_accuracy
    }

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    joblib.dump(artifact, output_path)

    logger.info("Saved model artifact to %s", output_path)
    print(f"\nSaved model artifact to {output_path}")


def run_random_forest_pipeline(
    df,
    feature_cols,
    target_col,
    dataset_name,
    feature_medians,
    model_output_path
):
    print(f"\n{'=' * 60}")
    print(f"RUNNING MODEL FOR: {dataset_name}")
    print(f"{'=' * 60}")

    X = df[feature_cols]
    y = df[target_col]

    print("\nTarget distribution:")
    print(y.value_counts())

    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=0.20,
        random_state=RANDOM_STATE,
        stratify=y
    )

    print(f"\nTraining rows: {len(X_train)}")
    print(f"Testing rows: {len(X_test)}")

    cv_strategy = StratifiedKFold(
        n_splits=5,
        shuffle=True,
        random_state=RANDOM_STATE
    )

    param_grid = {
        "n_estimators": [300, 500, 800],
        "max_depth": [5, 10, 20],
        "max_features": ["sqrt", "log2", None]
    }

    rf = RandomForestClassifier(random_state=RANDOM_STATE)

    grid_search = GridSearchCV(
        estimator=rf,
        param_grid=param_grid,
        cv=cv_strategy,
        scoring="accuracy",
        n_jobs=-1,
        verbose=1
    )

    grid_search.fit(X_train, y_train)

    best_model = grid_search.best_estimator_

    print("\nBest hyperparameters:")
    print(grid_search.best_params_)

    print(f"\nBest CV accuracy: {grid_search.best_score_:.4f}")

    y_pred = best_model.predict(X_test)
    test_accuracy = accuracy_score(y_test, y_pred)

    print(f"\nTest accuracy: {test_accuracy:.4f}")
    print("\nConfusion matrix:")
    print(confusion_matrix(y_test, y_pred))
    print("\nClassification report:")
    print(classification_report(y_test, y_pred))

    importances = pd.DataFrame({
        "feature": feature_cols,
        "importance": best_model.feature_importances_
    }).sort_values(by="importance", ascending=False)

    print("\nFeature importances:")
    print(importances)

    save_model_artifact(
        model=best_model,
        feature_cols=feature_cols,
        feature_medians=feature_medians,
        best_params=grid_search.best_params_,
        best_cv_score=grid_search.best_score_,
        test_accuracy=test_accuracy,
        output_path=model_output_path
    )

    return {
        "best_model": best_model,
        "best_params": grid_search.best_params_,
        "best_cv_score": grid_search.best_score_,
        "test_accuracy": test_accuracy,
        "feature_importances": importances
    }


# -------------------------------
# Main pipeline
# -------------------------------
def main():
    con = None

    try:
        con = duckdb.connect(database=DB_NAME, read_only=True)
        print(f"Connected to DuckDB database: {DB_NAME}")

        required_tables = [BREAKOUT_TABLE, REGRESSION_TABLE]
        for table_name in required_tables:
            if not table_exists(con, table_name):
                raise ValueError(
                    f"Required table '{table_name}' does not exist. "
                    "Run your transform scripts first."
                )

        breakout_df = load_table(con, BREAKOUT_TABLE)
        regression_df = load_table(con, REGRESSION_TABLE)

        breakout_clean, breakout_medians = preprocess_dataframe(
            breakout_df,
            FEATURE_COLUMNS,
            BREAKOUT_TARGET,
            "Breakout Dataset"
        )

        regression_clean, regression_medians = preprocess_dataframe(
            regression_df,
            FEATURE_COLUMNS,
            REGRESSION_TARGET,
            "Regression Dataset"
        )

        breakout_results = run_random_forest_pipeline(
            breakout_clean,
            FEATURE_COLUMNS,
            BREAKOUT_TARGET,
            "Breakout Dataset",
            breakout_medians,
            os.path.join(MODEL_DIR, "breakout_model.joblib")
        )

        regression_results = run_random_forest_pipeline(
            regression_clean,
            FEATURE_COLUMNS,
            REGRESSION_TARGET,
            "Regression Dataset",
            regression_medians,
            os.path.join(MODEL_DIR, "regression_model.joblib")
        )

        print(f"\n{'=' * 60}")
        print("FINAL SUMMARY")
        print(f"{'=' * 60}")
        print(f"Breakout best params: {breakout_results['best_params']}")
        print(f"Breakout best CV accuracy: {breakout_results['best_cv_score']:.4f}")
        print(f"Breakout test accuracy: {breakout_results['test_accuracy']:.4f}")

        print()

        print(f"Regression best params: {regression_results['best_params']}")
        print(f"Regression best CV accuracy: {regression_results['best_cv_score']:.4f}")
        print(f"Regression test accuracy: {regression_results['test_accuracy']:.4f}")

    except Exception as e:
        logger.error("An error occurred in analysis.py: %s", e)
        print(f"An error occurred: {e}")

    finally:
        if con:
            con.close()
            logger.info("DuckDB connection closed.")
            print("\nDuckDB connection closed.")


if __name__ == "__main__":
    main()

# predict.py

import logging
import os
import pandas as pd
import joblib

# -------------------------------
# Logging setup
# -------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="predict.log"
)
logger = logging.getLogger(__name__)

# -------------------------------
# Configuration
# -------------------------------
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

BREAKOUT_MODEL_PATH = os.path.join(BASE_DIR, "models", "breakout_model.joblib")
REGRESSION_MODEL_PATH = os.path.join(BASE_DIR, "models", "regression_model.joblib")

BREAKOUT_2025_PATH = os.path.join(BASE_DIR, "Data", "20-25(2025).csv")
REGRESSION_2025_PATH = os.path.join(BASE_DIR, "Data", "30-35(2025).csv")

PREDICTIONS_DIR = os.path.join(BASE_DIR, "predictions")

# -------------------------------
# Helper functions
# -------------------------------
def load_model_artifact(path):
    if not os.path.exists(path):
        raise FileNotFoundError(f"Model artifact not found: {path}")

    artifact = joblib.load(path)
    logger.info("Loaded model artifact from %s", path)
    return artifact


def preprocess_prediction_data(df, feature_cols, feature_medians, dataset_name):
    """
    Match the training preprocessing:
    - drop xwOBA if present
    - coerce features to numeric
    - impute with saved medians
    """
    if "xwOBA" in df.columns:
        df = df.drop(columns=["xwOBA"])
        logger.info("Dropped xwOBA column from %s.", dataset_name)

    working_df = df.copy()

    for col in feature_cols:
        if col not in working_df.columns:
            working_df[col] = pd.NA

    for col in feature_cols:
        working_df[col] = pd.to_numeric(working_df[col], errors="coerce")

    median_series = pd.Series(feature_medians)
    working_df[feature_cols] = working_df[feature_cols].fillna(median_series)

    return working_df


def predict_candidates(input_csv, model_artifact, dataset_name, output_csv):
    if not os.path.exists(input_csv):
        raise FileNotFoundError(f"Prediction input file not found: {input_csv}")

    df = pd.read_csv(input_csv)
    logger.info("Loaded %s with %d rows.", input_csv, len(df))

    model = model_artifact["model"]
    feature_cols = model_artifact["feature_columns"]
    feature_medians = model_artifact["feature_medians"]

    processed_df = preprocess_prediction_data(
        df,
        feature_cols,
        feature_medians,
        dataset_name
    )

    X_pred = processed_df[feature_cols]

    predicted_class = model.predict(X_pred)
    predicted_prob = model.predict_proba(X_pred)[:, 1]

    output_df = processed_df.copy()
    output_df["predicted_label"] = predicted_class
    output_df["predicted_probability"] = predicted_prob

    sort_col = "predicted_probability"
    output_df = output_df.sort_values(by=sort_col, ascending=False)

    os.makedirs(os.path.dirname(output_csv), exist_ok=True)
    output_df.to_csv(output_csv, index=False)

    print(f"\nTop predictions for {dataset_name}:")
    preview_cols = [col for col in ["Name", "Season", "predicted_label", "predicted_probability"] if col in output_df.columns]
    print(output_df[preview_cols].head(15))

    logger.info("Saved predictions for %s to %s", dataset_name, output_csv)


# -------------------------------
# Main prediction pipeline
# -------------------------------
def main():
    try:
        breakout_artifact = load_model_artifact(BREAKOUT_MODEL_PATH)
        regression_artifact = load_model_artifact(REGRESSION_MODEL_PATH)

        predict_candidates(
            input_csv=BREAKOUT_2025_PATH,
            model_artifact=breakout_artifact,
            dataset_name="Breakout Candidates (Age 20-25, 2025)",
            output_csv=os.path.join(PREDICTIONS_DIR, "breakout_predictions_2025.csv")
        )

        predict_candidates(
            input_csv=REGRESSION_2025_PATH,
            model_artifact=regression_artifact,
            dataset_name="Regression Candidates (Age 30-35, 2025)",
            output_csv=os.path.join(PREDICTIONS_DIR, "regression_predictions_2025.csv")
        )

        print("\nPrediction pipeline completed successfully.")

    except Exception as e:
        logger.error("An error occurred in predict.py: %s", e)
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    main()

# visualizations.py

import logging
import os
from xml.parsers.expat import model
import joblib
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import shap

# -------------------------------
# Logging setup
# -------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="visualization.log"
)
logger = logging.getLogger(__name__)

# -------------------------------
# Configuration
# -------------------------------
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

BREAKOUT_MODEL_PATH = os.path.join(BASE_DIR, "models", "breakout_model.joblib")
REGRESSION_MODEL_PATH = os.path.join(BASE_DIR, "models", "regression_model.joblib")

BREAKOUT_2025_PATH = os.path.join(BASE_DIR, "Data", "20-25(2025).csv")
REGRESSION_2025_PATH = os.path.join(BASE_DIR, "Data", "30-35(2025).csv")

OUTPUT_DIR = os.path.join(BASE_DIR, "predictions")

TOP_N = 5
TOP_FEATURES_PER_PLAYER = 5


# -------------------------------
# Helper functions
# -------------------------------
def load_model_artifact(path):
    if not os.path.exists(path):
        raise FileNotFoundError(f"Model artifact not found: {path}")

    artifact = joblib.load(path)
    logger.info("Loaded model artifact from %s", path)
    return artifact


def preprocess_prediction_data(df, feature_cols, feature_medians, dataset_name):
    """
    Match the training preprocessing:
    - drop xwOBA if present
    - coerce features to numeric
    - impute with saved medians
    """
    if "xwOBA" in df.columns:
        df = df.drop(columns=["xwOBA"])
        logger.info("Dropped xwOBA column from %s.", dataset_name)

    working_df = df.copy()

    for col in feature_cols:
        if col not in working_df.columns:
            working_df[col] = pd.NA

    for col in feature_cols:
        working_df[col] = pd.to_numeric(working_df[col], errors="coerce")

    median_series = pd.Series(feature_medians)
    working_df[feature_cols] = working_df[feature_cols].fillna(median_series)

    return working_df


def get_player_name_column(df):
    """
    Prefer Name if present, otherwise try common alternatives.
    """
    for col in ["Name", "player_name", "Player", "player"]:
        if col in df.columns:
            return col
    raise ValueError("Could not find a player name column in prediction data.")


def compute_predictions_and_shap(input_csv, model_artifact, dataset_name):
    """
    Load 2025 candidate data, preprocess it, generate predictions,
    and compute SHAP values.
    """
    if not os.path.exists(input_csv):
        raise FileNotFoundError(f"Prediction input file not found: {input_csv}")

    df = pd.read_csv(input_csv)
    logger.info("Loaded %s with %d rows.", input_csv, len(df))

    model = model_artifact["model"]
    feature_cols = model_artifact["feature_columns"]
    feature_medians = model_artifact["feature_medians"]

    processed_df = preprocess_prediction_data(
        df,
        feature_cols,
        feature_medians,
        dataset_name
    )

    X_pred = processed_df[feature_cols]

    predicted_prob = model.predict_proba(X_pred)[:, 1]
    predicted_class = model.predict(X_pred)

    output_df = processed_df.copy()
    output_df["predicted_probability"] = predicted_prob
    output_df["predicted_label"] = predicted_class

    explainer = shap.TreeExplainer(model)

    shap_explanation = explainer(X_pred)

    # shap_explanation.values shape:
    # (n_samples, n_features) OR (n_samples, n_features, n_classes)
    if len(shap_explanation.values.shape) == 3:
        shap_values_class1 = shap_explanation.values[:, :, 1]
    else:
        shap_values_class1 = shap_explanation.values

    return output_df, X_pred, shap_values_class1


def build_top_contribution_table(output_df, X_pred, shap_values, top_n, top_features_per_player):
    """
    Build a table for the top N predicted players, keeping only the top
    feature contributions by absolute SHAP value for each player.
    """
    name_col = get_player_name_column(output_df)

    ranked = output_df.sort_values(by="predicted_probability", ascending=False).head(top_n).copy()

    contribution_rows = []

    for idx in ranked.index:
        player_name = output_df.loc[idx, name_col]
        predicted_probability = output_df.loc[idx, "predicted_probability"]

        player_shap = shap_values[idx]
        player_features = X_pred.loc[idx]

        shap_series = pd.Series(player_shap, index=X_pred.columns)
        top_features = shap_series.abs().sort_values(ascending=False).head(top_features_per_player).index

        row = {
            "player_name": player_name,
            "predicted_probability": predicted_probability
        }

        for feature in top_features:
            row[f"{feature}_contribution"] = shap_series[feature]
            row[f"{feature}_value"] = player_features[feature]

        contribution_rows.append(row)

    return ranked, pd.DataFrame(contribution_rows)


def plot_stacked_shap_bars(output_df, X_pred, shap_values, title, output_path, top_n=5, top_features_per_player=5):
    """
    Create a stacked horizontal bar chart for the top N predicted players.
    Each bar is split into the top SHAP feature contributions for that player.
    Positive contributions push probability upward; negative contributions push it downward.
    """
    name_col = get_player_name_column(output_df)

    ranked = output_df.sort_values(by="predicted_probability", ascending=False).head(top_n).copy()

    player_names = ranked[name_col].tolist()
    player_probs = ranked["predicted_probability"].tolist()
    ranked_indices = ranked.index.tolist()

    plt.figure(figsize=(12, 8))

    # We'll manually stack positive and negative contributions separately
    y_positions = np.arange(len(player_names))

    # Collect all unique top features across the selected players
    feature_sets = []
    player_top_shap = {}

    for idx in ranked_indices:
        shap_series = pd.Series(shap_values[idx], index=X_pred.columns)
        top_features = shap_series.abs().sort_values(ascending=False).head(top_features_per_player)
        player_top_shap[idx] = top_features
        feature_sets.extend(top_features.index.tolist())

    unique_features = list(dict.fromkeys(feature_sets))

    # Use matplotlib default color cycle
    color_cycle = plt.rcParams["axes.prop_cycle"].by_key()["color"]
    feature_colors = {
        feature: color_cycle[i % len(color_cycle)]
        for i, feature in enumerate(unique_features)
    }

    # Plot stacked contributions
    for i, idx in enumerate(ranked_indices):
        shap_series = player_top_shap[idx]

        pos_left = 0.0
        neg_left = 0.0

        # Plot negative contributions first
        for feature, value in shap_series.items():
            if value < 0:
                plt.barh(
                    y=i,
                    width=value,
                    left=neg_left,
                    color=feature_colors[feature],
                    edgecolor="white",
                    label=feature if i == 0 else None
                )
                neg_left += value

        # Plot positive contributions
        for feature, value in shap_series.items():
            if value > 0:
                plt.barh(
                    y=i,
                    width=value,
                    left=pos_left,
                    color=feature_colors[feature],
                    edgecolor="white",
                    label=feature if i == 0 else None
                )
                pos_left += value

        # Add probability label at the right
        plt.text(
            x=max(pos_left, 0) + 0.01,
            y=i,
            s=f"p={player_probs[i]:.3f}",
            va="center"
        )

    plt.axvline(x=0, linewidth=1)

    plt.yticks(y_positions, player_names)
    plt.gca().invert_yaxis()

    plt.xlabel("SHAP contribution to positive-class prediction")
    plt.title(title)

    # Clean duplicate legend entries
    handles, labels = plt.gca().get_legend_handles_labels()
    dedup = dict(zip(labels, handles))
    plt.legend(dedup.values(), dedup.keys(), bbox_to_anchor=(1.02, 1), loc="upper left")

    plt.tight_layout()
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()

    logger.info("Saved SHAP stacked bar chart to %s", output_path)
    print(f"Saved chart to {output_path}")


# -------------------------------
# Main visualization pipeline
# -------------------------------
def main():
    try:
        breakout_artifact = load_model_artifact(BREAKOUT_MODEL_PATH)
        regression_artifact = load_model_artifact(REGRESSION_MODEL_PATH)

        breakout_output_df, breakout_X, breakout_shap = compute_predictions_and_shap(
            input_csv=BREAKOUT_2025_PATH,
            model_artifact=breakout_artifact,
            dataset_name="Breakout Candidates (Age 20-25, 2025)"
        )

        regression_output_df, regression_X, regression_shap = compute_predictions_and_shap(
            input_csv=REGRESSION_2025_PATH,
            model_artifact=regression_artifact,
            dataset_name="Regression Candidates (Age 30-35, 2025)"
        )

        plot_stacked_shap_bars(
            output_df=breakout_output_df,
            X_pred=breakout_X,
            shap_values=breakout_shap,
            title="Top 5 Predicted Breakout Candidates (2025 data) with SHAP Contributions",
            output_path=os.path.join(OUTPUT_DIR, "top5_breakout_shap.png"),
            top_n=TOP_N,
            top_features_per_player=TOP_FEATURES_PER_PLAYER
        )

        plot_stacked_shap_bars(
            output_df=regression_output_df,
            X_pred=regression_X,
            shap_values=regression_shap,
            title="Top 5 Predicted Regression Candidates (2025 data) with SHAP Contributions",
            output_path=os.path.join(OUTPUT_DIR, "top5_regression_shap.png"),
            top_n=TOP_N,
            top_features_per_player=TOP_FEATURES_PER_PLAYER
        )

        print("\nVisualization pipeline completed successfully.")

    except Exception as e:
        logger.error("An error occurred in visualization.py: %s", e)
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
