import duckdb
import polars as pl

pl.Config.set_tbl_formatting("ASCII_FULL")

DB_FILE = "strava.duckdb"
con = duckdb.connect(DB_FILE)

df = con.execute("SELECT * FROM bronze_activities").fetch_arrow_table()
print(df)