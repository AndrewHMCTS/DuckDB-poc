import duckdb
import os

DB_FILE = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "strava.duckdb")
)

print(f"Connecting to: {DB_FILE}")
con = duckdb.connect(DB_FILE)

# print(con.execute("SELECT *  FROM bronze_activities;").fetchdf())
print(con.execute("DESCRIBE bronze_activities;").fetchdf())
# print(con.execute("SHOW TABLES").fetchdf())
# con.execute("DELETE FROM bronze_activities;")