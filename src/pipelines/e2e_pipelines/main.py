import duckdb
import logging
import os
from dotenv import load_dotenv

from extract_from_api import run_extraction
from elt_to_s3 import run_elt
from create_dims import create_dim_tables
from connect_s3_motherduck import sync_to_motherduck

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

DB_FILE = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "strava.duckdb")
)
logger.info(f"Connecting to: {DB_FILE}")

def run():
    con = duckdb.connect(DB_FILE)

    try:
        logger.info("=" * 50)
        logger.info("PIPELINE START")

        logger.info("Stage 1: Extract from Strava API")
        has_new_data = run_extraction(con)

        if not has_new_data:
            logger.info("No new data — pipeline complete")
            return

        logger.info("Stage 2: ELT — raw → bronze → silver → gold → S3")
        run_elt(con)

        logger.info("Stage 2.5: Build dim/fact tables → S3")
        create_dim_tables(con)

        logger.info("Stage 3: Sync gold → MotherDuck")
        sync_to_motherduck()

        logger.info("PIPELINE COMPLETE ✅")

    except Exception as e:
        logger.error("PIPELINE FAILED ❌: %s", e)
        raise  # re-raise so GitHub Actions marks the run as failed

    finally:
        con.close()


if __name__ == "__main__":
    run()