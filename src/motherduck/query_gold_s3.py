import duckdb, os
from dotenv import load_dotenv

load_dotenv()

KEY_ID = os.getenv('S3_KEY_ID')
ACCESS_KEY = os.getenv('S3_ACCESS_KEY')

def query_s3_bucket() -> None:

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

        df = con.execute("""
            SELECT * 
            FROM read_parquet('s3://motherduck-test-am-testytest/gold/gold_monthly_stats.parquet')
        """).df()

        print(df)

if __name__ == "__main__":
    query_s3_bucket()

