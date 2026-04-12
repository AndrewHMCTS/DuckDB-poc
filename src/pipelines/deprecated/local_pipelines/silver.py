import duckdb

def create_silver_tables() -> None:

    DB_FILE = "strava.duckdb"

    with duckdb.connect(database=DB_FILE) as con:

        con.execute("""
        CREATE OR REPLACE TABLE silver_activities AS
        SELECT
            a.activity_id,
            a.name,
            m.distance / 1000.0 AS distance_km,
            m.moving_time / 60.0 AS moving_time_min,
            m.elapsed_time / 60.0 AS elapsed_time_min,
            CASE 
                WHEN m.distance > 0 
                THEN (m.moving_time / 60.0) / (m.distance / 1000.0)
                ELSE NULL
            END AS pace_min_per_km,
            m.total_elevation_gain AS total_elevation_m,
            m.average_speed,
            m.max_speed,
            m.elev_high,
            m.elev_low,
            a.type,
            a.sport_type,
            a.workout_type,
            a.device_name,
            a.start_date::TIMESTAMP AS start_date_utc,
            a.start_date_local::TIMESTAMP AS start_date_local,
            a.timezone,
            a.trainer,
            a.commute,
            a.manual,
            a.private,
            a.visibility,
            a.flagged,
            a.gear_id,
            a.upload_id,
            a.upload_id_str,
            a.external_id,
            a.from_accepted_tag,
            a.has_kudoed

        FROM bronze_activities a
        LEFT JOIN bronze_metrics m
            ON a.activity_id = m.activity_id

        WHERE a.activity_id IS NOT NULL
        """)

        con.execute("""
        CREATE OR REPLACE TABLE silver_athletes AS
        SELECT DISTINCT
            athlete_id,
            resource_state
        FROM bronze_athletes
        WHERE athlete_id IS NOT NULL
        """)

        con.execute("""
        CREATE OR REPLACE TABLE silver_heartrate AS
        SELECT
            activity_id,
            CASE WHEN has_heartrate THEN average_heartrate ELSE NULL END AS average_heartrate,
            CASE WHEN has_heartrate THEN max_heartrate ELSE NULL END AS max_heartrate,
            has_heartrate
        FROM bronze_heartrate
        WHERE activity_id IS NOT NULL
        """)

        con.execute("""
        CREATE OR REPLACE TABLE silver_map AS
        SELECT
            activity_id,
            map_id,
            summary_polyline
        FROM bronze_map
        WHERE activity_id IS NOT NULL
        """)

        con.execute("""
        CREATE OR REPLACE TABLE silver_engagement AS
        SELECT
            activity_id,
            achievement_count,
            kudos_count,
            comment_count,
            athlete_count,
            photo_count,
            total_photo_count,
            pr_count
        FROM bronze_engagement
        WHERE activity_id IS NOT NULL
        """)

        con.execute("""
        CREATE OR REPLACE TABLE silver_location AS
        SELECT
            activity_id,
            location_city,
            location_state,
            location_country
        FROM bronze_location
        WHERE activity_id IS NOT NULL
        """)

        con.execute("""
        CREATE OR REPLACE TABLE silver_activity_fact AS
        SELECT
            a.*,
            h.average_heartrate,
            h.max_heartrate,
            h.has_heartrate,

            e.kudos_count,
            e.comment_count,
            e.achievement_count,
            e.pr_count,

            l.location_city,
            l.location_state,
            l.location_country

        FROM silver_activities a
        LEFT JOIN silver_heartrate h
            ON a.activity_id = h.activity_id
        LEFT JOIN silver_engagement e
            ON a.activity_id = e.activity_id
        LEFT JOIN silver_location l
            ON a.activity_id = l.activity_id
        """)

        tables = con.execute("SHOW TABLES").fetchall()
        print("Tables in the database:", tables)
    
if __name__ == "__main__":
    create_silver_tables()