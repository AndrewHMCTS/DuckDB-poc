import duckdb
import logging

logging.basicConfig( 
    level=logging.INFO, 
    format="%(asctime)s [%(levelname)s] %(message)s" ) 

logger = logging.getLogger(__name__) 

con = duckdb.connect()

logger.info("Starting...")

con.execute("LOAD ducklake;")
con.execute("ATTACH 'ducklake:strava.duckdb.ducklake' AS strava_ducklake;")
con.execute("USE strava_ducklake;")

logger.info("Connecting to Data Lake 'strava_ducklake' ...")

snapshots = con.execute("FROM ducklake_snapshots('strava_ducklake');").fetchdf()
print(snapshots)

logger.info("Loading 1.72billion rows from nyc taxi data...")

con.execute("""
            CREATE TABLE IF NOT EXISTS nyc_taxis AS (
            SELECT COUNT(*)
            FROM 's3://us-prd-md-duckdb-in-action/nyc-taxis/yellow_tripdata_*-*.parquet');
""").fetchdf()

nyx_taxi_data = con.execute("SELECT * FROM nyc_taxis").fetchdf()
print(nyx_taxi_data)

logger.info("Retrieved 1.72billion rows from nyc taxi data...")
logger.info("Displaying Data Lake Metadata after ingesting data...")

snapshots = con.execute("FROM ducklake_snapshots('strava_ducklake');").fetchdf()
print(snapshots)

logger.info("Done")