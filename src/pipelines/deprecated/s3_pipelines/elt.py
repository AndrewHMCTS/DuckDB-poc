import duckdb, os
from dotenv import load_dotenv

load_dotenv()

KEY_ID = os.getenv('S3_KEY_ID')
ACCESS_KEY = os.getenv('S3_ACCESS_KEY')
BUCKET = 'motherduck-test-am-testytest'

def setup_s3_connection(con):
    """Setup S3 credentials"""
    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")
    con.execute(f"""
        CREATE SECRET (
            TYPE S3,
            KEY_ID '{KEY_ID}',
            SECRET '{ACCESS_KEY}',
            REGION 'eu-central-1',
            SCOPE 's3://{BUCKET}'
        )
    """)

def create_raw_tables(con):
    """Create raw tables from local JSON and upload to S3"""
    
    raw_files = [
        'raw_strava_activities',
        'raw_stats_for_athlete',
        'raw_athlete',
        'raw_strava_comments_partial',
        'raw_strava_kudos_partial'
    ]
    
    table_mapping = {
        'raw_strava_activities': 'raw_activities',
        'raw_strava_comments_partial': 'raw_strava_comments',
        'raw_strava_kudos_partial': 'raw_kudos',
        'raw_stats_for_athlete': 'raw_stats_for_athlete',
        'raw_athlete': 'raw_athlete'
    }
    
    for file in raw_files:
        table_name = table_mapping[file]
        print(f"Creating raw table: {table_name}")
        
        con.execute(f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT * FROM read_json_auto('raw/{file}.json')
        """)
        
        # Upload JSON to S3 raw layer
        con.execute(f"""
            COPY (SELECT * FROM read_json_auto('raw/{file}.json'))
            TO 's3://{BUCKET}/raw/{file}.json'
            (FORMAT JSON)
        """)
        
        # Also save as parquet in S3 raw layer
        con.execute(f"""
            COPY (SELECT * FROM {table_name})
            TO 's3://{BUCKET}/raw/{table_name}.parquet'
            (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
    
    print("Raw tables created and uploaded to S3")

def create_bronze_tables(con):
    """Create bronze tables and save to S3"""
    
    bronze_tables = {
        'bronze_activities': """
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
        """,
        'bronze_athletes': """
            SELECT DISTINCT
                athlete.id AS athlete_id,
                athlete.resource_state
            FROM raw_activities
        """,
        'bronze_heartrate': """
            SELECT
                id AS activity_id,
                has_heartrate,
                average_heartrate,
                max_heartrate,
                heartrate_opt_out,
                display_hide_heartrate_option
            FROM raw_activities
        """,
        'bronze_map': """
            SELECT
                id AS activity_id,
                map.id AS map_id,
                map.summary_polyline,
                map.resource_state
            FROM raw_activities
        """,
        'bronze_metrics': """
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
        """,
        'bronze_location': """
            SELECT
                id AS activity_id,
                location_city,
                location_state,
                location_country
            FROM raw_activities
        """,
        'bronze_engagement': """
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
        """,
        'bronze_geo': """
            SELECT
                id AS activity_id,
                start_latlng,
                end_latlng
            FROM raw_activities
        """
    }
    
    for table_name, query in bronze_tables.items():
        print(f"Creating bronze table: {table_name}")
        
        # Create local table
        con.execute(f"CREATE OR REPLACE TABLE {table_name} AS {query}")
        
        # Save to S3 bronze layer
        con.execute(f"""
            COPY (SELECT * FROM {table_name})
            TO 's3://{BUCKET}/bronze/{table_name}.parquet'
            (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
    
    print("Bronze tables created")

def create_silver_tables(con):
    """Create silver tables and save to S3"""
    
    silver_tables = {
        'silver_activities': """
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
            LEFT JOIN bronze_metrics m ON a.activity_id = m.activity_id
            WHERE a.activity_id IS NOT NULL
        """,
        'silver_athletes': """
            SELECT DISTINCT
                athlete_id,
                resource_state
            FROM bronze_athletes
            WHERE athlete_id IS NOT NULL
        """,
        'silver_heartrate': """
            SELECT
                activity_id,
                CASE WHEN has_heartrate THEN average_heartrate ELSE NULL END AS average_heartrate,
                CASE WHEN has_heartrate THEN max_heartrate ELSE NULL END AS max_heartrate,
                has_heartrate
            FROM bronze_heartrate
            WHERE activity_id IS NOT NULL
        """,
        'silver_map': """
            SELECT
                activity_id,
                map_id,
                summary_polyline
            FROM bronze_map
            WHERE activity_id IS NOT NULL
        """,
        'silver_engagement': """
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
        """,
        'silver_location': """
            SELECT
                activity_id,
                location_city,
                location_state,
                location_country
            FROM bronze_location
            WHERE activity_id IS NOT NULL
        """
    }
    
    for table_name, query in silver_tables.items():
        print(f"Creating silver table: {table_name}")
        
        # Create local table
        con.execute(f"CREATE OR REPLACE TABLE {table_name} AS {query}")
        
        # Save to S3 silver layer
        con.execute(f"""
            COPY (SELECT * FROM {table_name})
            TO 's3://{BUCKET}/silver/{table_name}.parquet'
            (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
    
    # Create silver fact table
    print("Creating silver_activity_fact")
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
        LEFT JOIN silver_heartrate h ON a.activity_id = h.activity_id
        LEFT JOIN silver_engagement e ON a.activity_id = e.activity_id
        LEFT JOIN silver_location l ON a.activity_id = l.activity_id
    """)
    
    con.execute(f"""
        COPY (SELECT * FROM silver_activity_fact)
        TO 's3://{BUCKET}/silver/silver_activity_fact.parquet'
        (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    
    print("Silver tables created")

def create_gold_tables(con):
    """Create gold tables and save to S3"""
    
    gold_tables = {
        'gold_activity_summary': """
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
        """,
        'gold_weekly_stats': """
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
        """,
        'gold_monthly_stats': """
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
        """,
        'gold_pb_efforts': """
            SELECT *
            FROM silver_activity_fact
            ORDER BY pace_min_per_km ASC
            LIMIT 10
        """,
        'gold_training_load': """
            SELECT
                activity_id,
                start_date_utc,
                distance_km,
                moving_time_min,
                average_heartrate,
                (moving_time_min * average_heartrate) AS effort_score
            FROM silver_activity_fact
            WHERE average_heartrate IS NOT NULL
        """
    }
    
    for table_name, query in gold_tables.items():
        print(f"Creating gold table: {table_name}")
        
        # Create local table
        con.execute(f"CREATE OR REPLACE TABLE {table_name} AS {query}")
        
        # Save to S3 gold layer
        con.execute(f"""
            COPY (SELECT * FROM {table_name})
            TO 's3://{BUCKET}/gold/{table_name}.parquet'
            (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
    
    print("Gold tables created")

def main():
    DB_FILE = "strava.duckdb"
    
    with duckdb.connect(database=DB_FILE) as con:
        print("Setting up S3 connection...")
        setup_s3_connection(con)
        
        print("\n=== RAW LAYER ===")
        create_raw_tables(con)
        
        print("\n=== BRONZE LAYER ===")
        create_bronze_tables(con)
        
        print("\n=== SILVER LAYER ===")
        create_silver_tables(con)
        
        print("\n=== GOLD LAYER ===")
        create_gold_tables(con)
        
        print("\n✅ Pipeline complete!")
        print(f"Local DB: {DB_FILE}")
        print(f"S3 Bucket: s3://{BUCKET}/")
        print("  - raw/")
        print("  - bronze/")
        print("  - silver/")
        print("  - gold/")

if __name__ == "__main__":
    main()