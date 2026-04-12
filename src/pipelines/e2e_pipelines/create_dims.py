import duckdb
import os
import logging
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

KEY_ID = os.getenv('S3_KEY_ID')
ACCESS_KEY = os.getenv('S3_ACCESS_KEY')
BUCKET = 'motherduck-test-am-testytest'

DB_FILE = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "strava.duckdb")
)

dim_tables = {
    'dim_date': """
        SELECT DISTINCT
            DATE_TRUNC('day', start_date_utc)::DATE AS date_id,
            YEAR(start_date_utc)                    AS year,
            MONTH(start_date_utc)                   AS month,
            DAY(start_date_utc)                     AS day,
            DAYNAME(start_date_utc)                 AS day_name,
            MONTHNAME(start_date_utc)               AS month_name,
            WEEK(start_date_utc)                    AS week_number,
            QUARTER(start_date_utc)                 AS quarter,
            DATE_TRUNC('week', start_date_utc)::DATE  AS week_start,
            DATE_TRUNC('month', start_date_utc)::DATE AS month_start
        FROM silver_activity_fact
    """,
    'dim_activity_type': """
        SELECT DISTINCT
            b.athlete_id,
            a.sport_type AS activity_type_id,
            a.sport_type AS sport_type,
            CASE a.sport_type
                WHEN 'Run'            THEN 'Cardio'
                WHEN 'Ride'           THEN 'Cardio'
                WHEN 'Swim'           THEN 'Cardio'
                WHEN 'WeightTraining' THEN 'Strength'
                WHEN 'Yoga'           THEN 'Flexibility'
                ELSE 'Other'
            END AS sport_category
        FROM silver_activity_fact a
        LEFT JOIN bronze_activities b ON a.activity_id = b.activity_id
        WHERE a.sport_type IS NOT NULL
    """,
    'dim_athlete': """
        SELECT DISTINCT
            b.athlete_id,
            'Andrew McDevitt' AS athlete_name
        FROM silver_activity_fact a
        LEFT JOIN bronze_activities b ON a.activity_id = b.activity_id

    """,
    'fact_activities': """
        SELECT
            a.activity_id,
            a.start_date_utc::DATE AS date_id,
            a.sport_type AS activity_type_id,
            b.athlete_id,
            a.name AS activity_name,
            a.distance_km,
            a.moving_time_min,
            a.elapsed_time_min,
            a.pace_min_per_km,
            a.total_elevation_m,
            a.average_heartrate,
            a.max_heartrate,
            a.average_speed,
            a.max_speed,
            a.kudos_count,
            a.comment_count,
            a.achievement_count,
            a.pr_count,
            a.trainer,
            a.commute,
            ROUND(a.distance_km * 0.621371, 2)      AS distance_miles,
            ROUND(a.total_elevation_m * 3.28084, 0) AS elevation_feet,
            CASE
                WHEN a.average_heartrate < 120 THEN 'Zone 1 — Easy'
                WHEN a.average_heartrate < 140 THEN 'Zone 2 — Aerobic'
                WHEN a.average_heartrate < 160 THEN 'Zone 3 — Tempo'
                WHEN a.average_heartrate < 175 THEN 'Zone 4 — Threshold'
                ELSE 'Zone 5 — Max'
            END AS hr_zone,
            (a.moving_time_min * a.average_heartrate) AS effort_score
        FROM silver_activity_fact a
        LEFT JOIN bronze_activities b ON a.activity_id = b.activity_id
        WHERE a.activity_id IS NOT NULL
"""
}


def create_dim_tables(con):
    for table_name, query in dim_tables.items():
        logger.info("Creating %s", table_name)
        con.execute(f"CREATE OR REPLACE TABLE {table_name} AS {query}")
        con.execute(f"""
            COPY (SELECT * FROM {table_name})
            TO 's3://{BUCKET}/gold/{table_name}.parquet'
            (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
        count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        logger.info("%s — %d rows", table_name, count)


def run():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )

    logger.info("Connecting to: %s", DB_FILE)
    con = duckdb.connect(DB_FILE)

    try:
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

        create_dim_tables(con)
        logger.info("Dim tables complete ✅")

    except Exception as e:
        logger.error("Failed: %s", e)
        raise

    finally:
        con.close()


if __name__ == "__main__":
    run()