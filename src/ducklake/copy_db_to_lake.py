import duckdb

con = duckdb.connect()
con.execute("LOAD ducklake;")
con.execute("ATTACH 'ducklake:strava.duckdb.ducklake' AS strava_ducklake;")
con.execute("ATTACH 'strava.duckdb' AS my_duckdb;")
con.execute("COPY FROM DATABASE my_duckdb TO strava_ducklake;")

print(con.execute("""
    SELECT database_name, schema_name, table_name 
    FROM duckdb_tables() 
    WHERE database_name = 'strava_ducklake'
""").fetchdf())