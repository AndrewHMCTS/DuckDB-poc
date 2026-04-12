import duckdb

con = duckdb.connect()

con.execute("ATTACH 'ducklake:strava.duckdb.ducklake' AS strava_ducklake;")

print(con.execute("""
    SELECT *
    FROM strava_ducklake.main.gold_monthly_stats;
""").fetchdf())

print(con.execute("""
    FROM ducklake_list_files('strava_ducklake', 'gold_monthly_stats');
""").fetchdf())
