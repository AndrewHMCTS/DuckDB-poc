import duckdb

def create_raw_tables() -> None:

    ##DuckDB should autoinfer the schema from the JSON file
    DB_FILE = "strava.duckdb"

    with duckdb.connect(database=DB_FILE) as con:

        con.execute("""
            CREATE OR REPLACE TABLE raw_activities AS
            SELECT *
            FROM read_json_auto('raw/raw_strava_activities.json')
        """)

        con.execute("""
            CREATE OR REPLACE TABLE raw_stats_for_athlete AS
            SELECT *
            FROM read_json_auto('raw/raw_stats_for_athlete.json')
        """)

        con.execute("""
            CREATE OR REPLACE TABLE raw_athlete AS
            SELECT *
            FROM read_json_auto('raw/raw_athlete.json')
        """)

        con.execute("""
            CREATE OR REPLACE TABLE raw_strava_comments AS
            SELECT *
            FROM read_json_auto('raw/raw_strava_comments_partial.json')
        """)

        con.execute("""
            CREATE OR REPLACE TABLE raw_kudos AS
            SELECT *
            FROM read_json_auto('raw/raw_strava_kudos_partial.json')
        """)

        tables = con.execute("SHOW TABLES").fetchall()
        print("Tables in the database:", tables)

if __name__ == "__main__":
    create_raw_tables()
