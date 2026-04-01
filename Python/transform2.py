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