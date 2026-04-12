import duckdb
import os
import logging
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

MOTHERDUCK_TOKEN = os.getenv('MOTHERDUCK_ACCESS_TOKEN')
KEY_ID = os.getenv('S3_KEY_ID')
ACCESS_KEY = os.getenv('S3_ACCESS_KEY')
BUCKET = 'motherduck-test-am-testytest'
REGION = 'eu-central-1'

GOLD_TABLES = [
    'gold_monthly_stats',
    'gold_training_load',
    'gold_activity_summary',
    'gold_weekly_stats',
    'gold_pb_efforts',
    'dim_date',
    'dim_activity_type',
    'dim_athlete',
    'fact_activities'
]

def sync_to_motherduck():
    """
    Stage 3: Materialise gold S3 Parquet → MotherDuck tables.
    Creates both a view (live S3 reads) and a materialised table
    (stable for Superset). Table is what Superset should query.
    """
    con = duckdb.connect(f"md:strava?motherduck_token={MOTHERDUCK_TOKEN}")

    con.execute("INSTALL httpfs; LOAD httpfs")

    # IF NOT EXISTS — avoids error if secret already registered
    con.execute(f"""
        CREATE SECRET IF NOT EXISTS s3_strava (
            TYPE S3,
            KEY_ID '{KEY_ID}',
            SECRET '{ACCESS_KEY}',
            REGION '{REGION}',
            SCOPE 's3://{BUCKET}'
        )
    """)

    logger.info("Secrets registered:\n%s", con.execute("FROM duckdb_secrets()").fetchdf().to_string())

    con.execute("CREATE SCHEMA IF NOT EXISTS gold")

    for table in GOLD_TABLES:
        s3_path = f"s3://{BUCKET}/gold/{table}.parquet"

        con.execute(f"""
            CREATE OR REPLACE TABLE gold.{table}_table AS
            SELECT * FROM '{s3_path}'
        """)
        logger.info("Table materialised: gold.%s_table → MotherDuck", table)

    result = con.execute("""
        SELECT table_schema, table_name, table_type
        FROM information_schema.tables
        WHERE table_schema = 'gold'
        ORDER BY table_name
    """).fetchdf()

    logger.info("MotherDuck gold layer:\n%s", result.to_string())
    con.close()
    logger.info("MotherDuck sync complete ✅")