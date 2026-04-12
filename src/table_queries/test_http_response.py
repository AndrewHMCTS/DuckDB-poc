import os
import requests
import polars as pl
from dotenv import load_dotenv
import json

load_dotenv()

STRAVA_ACCESS_TOKEN = os.getenv("STRAVA_ACCESS_TOKEN")

response = requests.get(
    "https://www.strava.com/api/v3/athlete/activities/",
    headers={"Authorization": f"Bearer {STRAVA_ACCESS_TOKEN}"},
    params={"page": 1, "per_page": 5}
)

print("HTTP status code:", response.status_code)
print("HTTP status text:", response.text)

data = response.json()

print(json.dumps(data, indent=2))
