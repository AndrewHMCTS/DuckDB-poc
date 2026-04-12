import duckdb
import os
from dotenv import load_dotenv

load_dotenv()

os.environ["MOTHERDUCK_TOKEN"] = os.getenv('MOTHERDUCK_ACCESS_TOKEN')
AZURE_SAS_TOKEN = os.getenv('AZURE_SAS_TOKEN')
CNXN_STRING = os.getenv("CNXN_STRING")

AZURE_STORAGE_ACCOUNT = "motherducktest111"
AZURE_CONTAINER = "mdcontainer"
AZURE_PATH = "layer1/"

# Step 1: Create DuckLake in MotherDuck backed by Azure Blob Storage
con = duckdb.connect("md:")
con.execute("LOAD ducklake;")
con.execute("INSTALL azure;")
con.execute("LOAD azure;")

con.execute(f"""
    CREATE OR REPLACE SECRET azure_secret IN MOTHERDUCK (
        TYPE AZURE,
        CONNECTION_STRING '{CNXN_STRING}'
    );
""")

con.execute(f"""
    CREATE DATABASE strava_lake (
        TYPE DUCKLAKE,
        DATA_PATH 'abfss://{AZURE_CONTAINER}@{AZURE_STORAGE_ACCOUNT}.dfs.core.windows.net/{AZURE_PATH}'
    );
""")
con.close()

# Step 2: Copy local DuckLake into MotherDuck DuckLake
# con = duckdb.connect()
# con.execute("LOAD ducklake;")
# con.execute("ATTACH 'ducklake:strava.duckdb.ducklake' AS local_strava;")
# con.execute("ATTACH 'md:strava_lake' AS remote_strava;")
# con.execute("COPY FROM DATABASE local_strava TO remote_strava;")

# print(con.execute("""
#     SELECT database_name, schema_name, table_name
#     FROM duckdb_tables()
#     WHERE database_name = 'remote_strava'
# """).fetchdf())

# con.close()