import logging
import time

import duckdb

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

FILES = [
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-02.parquet",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-03.parquet",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-04.parquet",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-05.parquet",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-06.parquet",
]


def benchmark(con, name: str, query: str) -> float:
    logger.info("Running: %s", name)
    start = time.time()
    result = con.execute(query).fetchdf()
    elapsed = time.time() - start
    logger.info("%-40s → %.2fs\n%s", name, elapsed, result.to_string())
    return elapsed


def run():
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs")

    files_sql = str(FILES)

    results = {}

    results["row_count"] = benchmark(
        con,
        "Row count",
        f"""
        SELECT COUNT(*) AS total_trips
        FROM read_parquet({files_sql}, union_by_name=true)
    """,
    )

    results["revenue_by_month"] = benchmark(
        con,
        "Revenue by month",
        f"""
        SELECT
            MONTH(tpep_pickup_datetime)  AS month,
            COUNT(*)                     AS trips,
            ROUND(SUM(total_amount), 2)  AS total_revenue,
            ROUND(AVG(total_amount), 2)  AS avg_fare
        FROM read_parquet({files_sql}, union_by_name=true)
        WHERE total_amount > 0
        GROUP BY 1
        ORDER BY 1
    """,
    )

    time.sleep(3)

    results["busiest_hours"] = benchmark(
        con,
        "Busiest pickup hours",
        f"""
        SELECT
            HOUR(tpep_pickup_datetime)   AS hour_of_day,
            COUNT(*)                     AS trips,
            ROUND(AVG(trip_distance), 2) AS avg_distance_miles
        FROM read_parquet({files_sql}, union_by_name=true)
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT 5
    """,
    )

    time.sleep(5)

    results["tip_analysis"] = benchmark(
        con,
        "Tip analysis",
        f"""
        SELECT
            passenger_count,
            COUNT(*)                    AS trips,
            ROUND(AVG(tip_amount), 2)   AS avg_tip,
            ROUND(AVG(tip_amount /
                NULLIF(fare_amount, 0) * 100
            ), 2)                       AS avg_tip_pct
        FROM read_parquet({files_sql}, union_by_name=true)
        WHERE fare_amount > 0
          AND passenger_count > 0
        GROUP BY 1
        ORDER BY 1
    """,
    )

    time.sleep(5)

    results["payment_types"] = benchmark(
        con,
        "Payment type breakdown",
        f"""
        SELECT
            CASE payment_type
                WHEN 1 THEN 'Credit Card'
                WHEN 2 THEN 'Cash'
                WHEN 3 THEN 'No Charge'
                WHEN 4 THEN 'Dispute'
                ELSE 'Unknown'
            END AS payment_method,
            COUNT(*)                    AS trips,
            ROUND(SUM(total_amount), 2) AS total_revenue
        FROM read_parquet({files_sql}, union_by_name=true)
        GROUP BY 1
        ORDER BY 2 DESC
    """,
    )

    time.sleep(5)

    logger.info("=" * 50)
    logger.info("BENCHMARK SUMMARY")
    logger.info("=" * 50)
    for name, elapsed in results.items():
        logger.info("%-40s %.2fs", name, elapsed)
    logger.info("Total: %.2fs", sum(results.values()))
    logger.info("=" * 50)
    logger.info("8 files — MacBook M4 — no cluster, no Spark, no DBUs")

    con.close()


if __name__ == "__main__":
    run()
