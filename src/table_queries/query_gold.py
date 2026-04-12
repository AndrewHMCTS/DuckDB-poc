import duckdb
import polars as pl

pl.Config.set_tbl_formatting("ASCII_FULL")

DB_FILE = "strava.duckdb"
con = duckdb.connect(DB_FILE)

def query_table(table_name: str) -> pl.DataFrame:
    tables = [t[0] for t in con.execute("SHOW TABLES").fetchall()]
    if table_name not in tables:
        raise ValueError(f"Table '{table_name}' does not exist. Available tables: {tables}")
    
    arrow_table = con.execute(f"SELECT * FROM {table_name}").fetch_arrow_table()
    return pl.from_arrow(arrow_table)

def summarise_table(table_name: str) -> pl.DataFrame:
    tables = [t[0] for t in con.execute("SHOW TABLES").fetchall()]
    if table_name not in tables:
        raise ValueError(f"Table '{table_name}' does not exist. Available tables: {tables}")
    
    arrow_table = con.execute(f"""
                              SUMMARIZE {table_name}""").fetch_arrow_table()
    return pl.from_arrow(arrow_table)

if __name__ == "__main__":
    df = query_table("silver_activities")
    df1 = query_table("silver_heartrate")
    df2 = query_table("gold_activity_summary")
    df3 = query_table("gold_monthly_stats")
    df4 = summarise_table("gold_activity_summary")
    print(df2)
    print(df3)
    
    max_hr = df3["max_heartrate"].max()
    print("Maximum max_heartrate:", max_hr)
    
    # # Filter the row(s) where max_heartrate equals the max value
    row_max_hr = df3.filter(pl.col("max_heartrate") == max_hr)
    
    # print("Row(s) causing max_heartrate:")
    print(row_max_hr)
