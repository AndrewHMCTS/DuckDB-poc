import duckdb

def create_gold_tables() -> None:

    DB_FILE = "strava.duckdb"

    with duckdb.connect(database=DB_FILE) as con:

        ##gold activity summary
        con.execute("""
        CREATE OR REPLACE TABLE gold_activity_summary AS
        SELECT
            activity_id,
            name,
            sport_type,
            start_date_utc,
            distance_km,
            moving_time_min,
            pace_min_per_km,
            total_elevation_m,
            average_heartrate,
            max_heartrate,
            kudos_count,
            comment_count,
            pr_count,
            trainer,
            commute,
            DATE_TRUNC('week', start_date_utc) AS week_start,
            DATE_TRUNC('month', start_date_utc) AS month_start

        FROM silver_activity_fact
        """)

        ##gold weekly stats
        con.execute("""
        CREATE OR REPLACE TABLE gold_weekly_stats AS
        SELECT
            DATE_TRUNC('week', start_date_utc) AS week_start,
            COUNT(*) AS activity_count,
            SUM(distance_km) AS total_distance_km,
            SUM(moving_time_min) AS total_time_min,
            AVG(pace_min_per_km) AS avg_pace,
            MIN(pace_min_per_km) AS best_pace,
            AVG(average_heartrate) AS avg_heartrate,
            MAX(max_heartrate) AS max_heartrate,
            SUM(total_elevation_m) AS total_elevation_m,
            SUM(kudos_count) AS total_kudos,
            SUM(comment_count) AS total_comments,
            SUM(pr_count) AS total_prs

        FROM silver_activity_fact
        GROUP BY 1
        ORDER BY 1
        """)

        ##gold monthly stats
        con.execute("""
        CREATE OR REPLACE TABLE gold_monthly_stats AS
        SELECT
            DATE_TRUNC('month', start_date_utc) AS month_start,
            COUNT(*) AS activity_count,
            SUM(distance_km) AS total_distance_km,
            SUM(moving_time_min) AS total_time_min,
            AVG(pace_min_per_km) AS avg_pace,
            MIN(pace_min_per_km) AS best_pace,
            AVG(average_heartrate) AS avg_heartrate,
            MAX(max_heartrate) AS max_heartrate,
            SUM(total_elevation_m) AS total_elevation_m,
            SUM(kudos_count) AS total_kudos,
            SUM(pr_count) AS total_prs

        FROM silver_activity_fact
        GROUP BY 1
        ORDER BY 1
        """)

        ##gold pb efforts
        con.execute("""
        CREATE OR REPLACE TABLE gold_pb_efforts AS
        SELECT *
        FROM silver_activity_fact
        ORDER BY pace_min_per_km ASC
        LIMIT 10
        """)

        ##gold training load
        con.execute("""
        CREATE OR REPLACE TABLE gold_training_load AS
        SELECT
            activity_id,
            start_date_utc,
            distance_km,
            moving_time_min,
            average_heartrate,
            (moving_time_min * average_heartrate) AS effort_score

        FROM silver_activity_fact
        WHERE average_heartrate IS NOT NULL
        """)

        tables = con.execute("SHOW TABLES").fetchall()
        print("Tables in the database:", tables)

if __name__ == "__main__":
    create_gold_tables()