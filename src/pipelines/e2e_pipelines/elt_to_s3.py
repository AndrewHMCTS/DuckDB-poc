import duckdb
import os
import logging
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

KEY_ID = os.getenv('S3_KEY_ID')
ACCESS_KEY = os.getenv('S3_ACCESS_KEY')
BUCKET = 'motherduck-test-am-testytest'

def setup_s3_connection(con):
    con.execute("INSTALL httpfs; LOAD httpfs")
    con.execute(f"""
        CREATE SECRET IF NOT EXISTS s3_strava (
            TYPE S3,
            KEY_ID '{KEY_ID}',
            SECRET '{ACCESS_KEY}',
            REGION 'eu-central-1',
            SCOPE 's3://{BUCKET}'
        )
    """)

def validate_counts(con, source: str, target: str):
    """Log row counts between layers, raise if target is empty"""
    src = con.execute(f"SELECT COUNT(*) FROM {source}").fetchone()[0]
    tgt = con.execute(f"SELECT COUNT(*) FROM {target}").fetchone()[0]
    logger.info("%-30s → %-30s  %d → %d rows", source, target, src, tgt)
    if tgt == 0:
        raise ValueError(f"{target} has 0 rows — aborting pipeline")


def create_raw_tables(con):
    raw_files = {
        'raw_strava_activities': 'raw_activities',
        'raw_strava_comments_partial': 'raw_strava_comments',
        'raw_strava_kudos_partial': 'raw_kudos',
        'raw_stats_for_athlete': 'raw_stats_for_athlete',
        'raw_athlete': 'raw_athlete'
    }

    for file, table_name in raw_files.items():
        logger.info("Loading raw table: %s", table_name)

        # Always full refresh raw local tables from JSON
        con.execute(f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT * FROM read_json_auto('raw/{file}.json')
        """)

        # Upload to S3 raw layer
        con.execute(f"""
            COPY (SELECT * FROM {table_name})
            TO 's3://{BUCKET}/raw/{table_name}.parquet'
            (FORMAT PARQUET, COMPRESSION ZSTD)
        """)

    logger.info("Raw layer complete")

def create_bronze_tables(con):
    """
    Bronze is append-only + deduplicated on activity_id.
    New records are inserted, existing records are never overwritten.
    """

    # Create bronze tables if they don't exist yet (first run)
    con.execute("""
        CREATE TABLE IF NOT EXISTS bronze_activities (
            activity_id BIGINT PRIMARY KEY,
            athlete_id BIGINT,
            name VARCHAR,
            type VARCHAR,
            sport_type VARCHAR,
            workout_type INTEGER,
            device_name VARCHAR,
            start_date VARCHAR,
            start_date_local VARCHAR,
            timezone VARCHAR,
            utc_offset DOUBLE,
            trainer BOOLEAN,
            commute BOOLEAN,
            manual BOOLEAN,
            private BOOLEAN,
            visibility VARCHAR,
            flagged BOOLEAN,
            gear_id VARCHAR,
            upload_id BIGINT,
            upload_id_str VARCHAR,
            external_id VARCHAR,
            from_accepted_tag BOOLEAN,
            has_kudoed BOOLEAN
        )
    """)

    # Insert only new records — deduplication on activity_id
    con.execute("""
        INSERT INTO bronze_activities
        SELECT
            id AS activity_id,
            athlete.id AS athlete_id,
            name, type, sport_type, workout_type, device_name,
            start_date, start_date_local, timezone, utc_offset,
            trainer, commute, manual, private, visibility, flagged,
            gear_id, upload_id, upload_id_str, external_id,
            from_accepted_tag, has_kudoed
        FROM raw_activities
        WHERE id NOT IN (SELECT activity_id FROM bronze_activities)
    """)

    validate_counts(con, "raw_activities", "bronze_activities")

    con.execute(f"""
        COPY (SELECT * FROM bronze_activities)
        TO 's3://{BUCKET}/bronze/bronze_activities.parquet'
        (FORMAT PARQUET, COMPRESSION ZSTD)
    """)

    # Remaining bronze tables — these are safe to full refresh
    # as they're derived from bronze_activities which is already deduped
    other_bronze = {
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
        'bronze_location': """
            SELECT
                id AS activity_id,
                location_city,
                location_state,
                location_country
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
        'bronze_geo': """
            SELECT
                id AS activity_id,
                start_latlng,
                end_latlng
            FROM raw_activities
        """
    }

    for table_name, query in other_bronze.items():
        logger.info("Creating bronze table: %s", table_name)
        con.execute(f"CREATE OR REPLACE TABLE {table_name} AS {query}")
        con.execute(f"""
            COPY (SELECT * FROM {table_name})
            TO 's3://{BUCKET}/bronze/{table_name}.parquet'
            (FORMAT PARQUET, COMPRESSION ZSTD)
        """)

    logger.info("Bronze layer complete")

def create_silver_tables(con):
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
                a.gear_id
            FROM bronze_activities a
            LEFT JOIN bronze_metrics m ON a.activity_id = m.activity_id
            WHERE a.activity_id IS NOT NULL
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
        logger.info("Creating silver table: %s", table_name)
        con.execute(f"CREATE OR REPLACE TABLE {table_name} AS {query}")
        con.execute(f"""
            COPY (SELECT * FROM {table_name})
            TO 's3://{BUCKET}/silver/{table_name}.parquet'
            (FORMAT PARQUET, COMPRESSION ZSTD)
        """)

    # Silver fact — full refresh, always rebuilt from all bronze
    logger.info("Creating silver_activity_fact")
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

    validate_counts(con, "bronze_activities", "silver_activity_fact")

    con.execute(f"""
        COPY (SELECT * FROM silver_activity_fact)
        TO 's3://{BUCKET}/silver/silver_activity_fact.parquet'
        (FORMAT PARQUET, COMPRESSION ZSTD)
    """)

    logger.info("Silver layer complete")

def create_gold_tables(con):
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
        logger.info("Creating gold table: %s", table_name)
        con.execute(f"CREATE OR REPLACE TABLE {table_name} AS {query}")
        con.execute(f"""
            COPY (SELECT * FROM {table_name})
            TO 's3://{BUCKET}/gold/{table_name}.parquet'
            (FORMAT PARQUET, COMPRESSION ZSTD)
        """)

    logger.info("Gold layer complete")

def run_elt(con):
    """
    Stage 2: Transform raw JSON → bronze → silver → gold → S3
    Accepts an existing connection so main.py controls the lifecycle.
    """
    setup_s3_connection(con)
    create_raw_tables(con)
    create_bronze_tables(con)
    create_silver_tables(con)
    create_gold_tables(con)
    logger.info("ELT complete")