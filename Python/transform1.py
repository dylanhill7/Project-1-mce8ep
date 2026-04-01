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
        # Final success message
        # -------------------------------
        logger.info("Finished transforming staging tables into 6 relational tables.")
        print("Finished transforming staging tables into 6 relational tables.")

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