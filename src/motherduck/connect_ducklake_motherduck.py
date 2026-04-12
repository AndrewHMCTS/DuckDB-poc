import duckdb
import os
from dotenv import load_dotenv

load_dotenv()

MOTHERDUCK_TOKEN = os.getenv('MOTHERDUCK_ACCESS_TOKEN')
KEY_ID = os.getenv('S3_KEY_ID')
ACCESS_KEY = os.getenv('S3_ACCESS_KEY')
BUCKET = 'motherduck-test-am-testytest'
REGION = 'eu-central-1'

# Connect directly to MotherDuck — no local file needed
con = duckdb.connect(f"md:strava?motherduck_token={MOTHERDUCK_TOKEN}")

# S3 credentials (MotherDuck needs these to reach your bucket)
con.execute("INSTALL httpfs; LOAD httpfs")
con.execute(f"""
    CREATE SECRET IF NOT EXISTS s3_strava (
        TYPE S3,
        KEY_ID '{KEY_ID}',
        SECRET '{ACCESS_KEY}',
        REGION '{REGION}',
        SCOPE 's3://{BUCKET}'
    )
""")

print(con.execute("FROM duckdb_secrets()").fetchdf())

# Your gold tables in S3
gold_tables = [
    'gold_monthly_stats',
    'gold_training_load',
    'gold_activity_summary',
    'gold_weekly_stats',
    'gold_pb_efforts'
]

# Register each as a view in MotherDuck — data stays in S3
con.execute("CREATE SCHEMA IF NOT EXISTS gold")

for table in gold_tables:
    s3_path = f"s3://{BUCKET}/gold/{table}.parquet"
    con.execute(f"""
        CREATE OR REPLACE VIEW gold.{table}_view AS
        SELECT * FROM '{s3_path}'
    """)
    print(f"Registered View: gold.{table} → {s3_path}")

for table in gold_tables:
    s3_path = f"s3://{BUCKET}/gold/{table}.parquet"
    con.execute(f"""
        CREATE OR REPLACE TABLE gold.{table}_table AS
        SELECT * FROM '{s3_path}'
    """)
    print(f"Registered Table: gold.{table} → {s3_path}")

# Verify
print(con.execute("""
    SELECT table_schema, table_name, table_type
    FROM information_schema.tables
    WHERE table_schema = 'gold'
""").fetchdf())