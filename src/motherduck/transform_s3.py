import duckdb, os
from dotenv import load_dotenv

load_dotenv()

KEY_ID = os.getenv('S3_KEY_ID')
ACCESS_KEY = os.getenv('S3_ACCESS_KEY')

def transform_s3_raw() -> None:

    with duckdb.connect() as con:

        con.execute("INSTALL httpfs")
        con.execute("LOAD httpfs")

        # Create in-session secret
        con.execute(f"""
            CREATE SECRET (
                TYPE S3,
                KEY_ID '{KEY_ID}',
                SECRET '{ACCESS_KEY}',
                REGION 'eu-central-1',
                SCOPE 's3://motherduck-test-am-testytest'
            )
        """)

        ##convert .json to .parquet & query
        raw_tables = ['raw_athlete', 'raw_stats_for_athlete', 'raw_strava_activities', 'raw_strava_comments_partial', 'raw_strava_kudos_partial']

        for table in raw_tables:
            con.execute(f"""
                COPY (
                    SELECT * FROM read_json_auto('s3://motherduck-test-am-testytest/{table}.json')
                ) TO 's3://motherduck-test-am-testytest/{table}.parquet' (FORMAT PARQUET)
            """)

            df = con.execute(f"""
                SELECT * FROM read_parquet('s3://motherduck-test-am-testytest/{table}.parquet')
            """).df()

            print(df)

if __name__ == "__main__":
    transform_s3_raw()
