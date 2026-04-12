import duckdb

def create_bronze_tables() -> None:

    DB_FILE = "strava.duckdb"

    with duckdb.connect(database=DB_FILE) as con:

        #Bronze activites
        con.execute("""
        CREATE OR REPLACE TABLE bronze_activities AS
        SELECT
            id AS activity_id,
            athlete.id AS athlete_id,
            name,
            type,
            sport_type,
            workout_type,
            device_name,
            start_date,
            start_date_local,
            timezone,
            utc_offset,
            trainer,
            commute,
            manual,
            private,
            visibility,
            flagged,
            gear_id,
            upload_id,
            upload_id_str,
            external_id,
            from_accepted_tag,
            has_kudoed
        FROM raw_activities
        """)

        #Bronze athletes
        con.execute("""
        CREATE OR REPLACE TABLE bronze_athletes AS
        SELECT DISTINCT
            athlete.id AS athlete_id,
            athlete.resource_state
        FROM raw_activities
        """)

        # Bronze HR
        con.execute("""
        CREATE OR REPLACE TABLE bronze_heartrate AS
        SELECT
            id AS activity_id,
            has_heartrate,
            average_heartrate,
            max_heartrate,
            heartrate_opt_out,
            display_hide_heartrate_option
        FROM raw_activities
        """)

        #Bronze map
        con.execute("""
        CREATE OR REPLACE TABLE bronze_map AS
        SELECT
            id AS activity_id,
            map.id AS map_id,
            map.summary_polyline,
            map.resource_state
        FROM raw_activities
        """)

        #Bronze metrics
        con.execute("""
        CREATE OR REPLACE TABLE bronze_metrics AS
        SELECT
            id AS activity_id,
            distance,
            moving_time,
            elapsed_time,
            total_elevation_gain,
            average_speed,
            max_speed,
            elev_high,
            elev_low
        FROM raw_activities
        """)

        #Bronze location
        con.execute("""
        CREATE OR REPLACE TABLE bronze_location AS
        SELECT
            id AS activity_id,
            location_city,
            location_state,
            location_country
        FROM raw_activities
        """)

        #Bronze engagement
        con.execute("""
        CREATE OR REPLACE TABLE bronze_engagement AS
        SELECT
            id AS activity_id,
            achievement_count,
            kudos_count,
            comment_count,
            athlete_count,
            photo_count,
            total_photo_count,
            pr_count
        FROM raw_activities
        """)

        #Bronze geo
        con.execute("""
        CREATE OR REPLACE TABLE bronze_geo AS
        SELECT
            id AS activity_id,
            start_latlng,
            end_latlng
        FROM raw_activities
        """)

        tables = con.execute("SHOW TABLES").fetchall()
        print("Tables in the database:", tables)

if __name__ == "__main__":
    create_bronze_tables()