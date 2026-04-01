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