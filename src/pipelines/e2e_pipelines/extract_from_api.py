import json
import logging
import os
import requests
import time
from datetime import datetime, timedelta, timezone
from typing import Optional
from dotenv import load_dotenv
import os

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

RAW_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "raw")
)
os.makedirs(RAW_DIR, exist_ok=True)
logger.info("RAW_DIR: %s", RAW_DIR)

class StravaClient:
    BASE_URL = "https://www.strava.com/api/v3"

    def __init__(self):
        self._token = None
        self._token_expiry = None

    @property
    def token(self) -> str:
        if self._token is None or self._is_expired():
            logger.info("Refreshing Strava access token...")
            self._refresh_token()
        return self._token

    def _is_expired(self) -> bool:
        if self._token_expiry is None:
            return True
        return datetime.now(timezone.utc) >= self._token_expiry

    def _refresh_token(self):
        data = self._get_token()
        self._token = data["access_token"]
        expires_at = data["expires_at"]
        self._token_expiry = datetime.fromtimestamp(
            expires_at, tz=timezone.utc
        ) - timedelta(seconds=60)
        if "refresh_token" in data:
            os.environ["STRAVA_REFRESH_TOKEN"] = data["refresh_token"]
        logger.info("Token refreshed. Expires at %s", self._token_expiry)

    def _get_token(self) -> dict:
        response = requests.post(
            f"{self.BASE_URL}/oauth/token",
            data={
                "client_id": os.getenv("STRAVA_CLIENT_ID"),
                "client_secret": os.getenv("STRAVA_CLIENT_SECRET"),
                "refresh_token": os.getenv("STRAVA_REFRESH_TOKEN"),
                "grant_type": "refresh_token",
            }
        )
        response.raise_for_status()
        return response.json()

    def _get(self, path: str, params: dict = None) -> dict:
        response = requests.get(
            f"{self.BASE_URL}{path}",
            headers={"Authorization": f"Bearer {self.token}"},
            params=params or {},
        )
        response.raise_for_status()
        return response.json()

    def get_activities(self, after: Optional[datetime] = None) -> list:
        """
        Fetch activities. If after is provided, only fetch activities
        newer than that datetime — incremental load.
        If None, fetch all activities — full load (first run).
        """
        params = {"per_page": 200}

        if after:
            # Strava expects unix timestamp
            params["after"] = int(after.timestamp())
            logger.info("Incremental load — after %s", after.isoformat())
        else:
            logger.info("Full load — fetching all activities")

        activities, page = [], 1
        while True:
            params["page"] = page
            batch = self._get("/athlete/activities", params)
            if not batch:
                break
            activities.extend(batch)
            logger.info("Page %d — %d activities", page, len(batch))
            page += 1

        logger.info("Total fetched: %d activities", len(activities))
        return activities

    def get_athlete(self) -> dict:
        results = self._get("/athlete")
        logger.info(
            "Athlete %s %s — id: %d",
            results.get("firstname"),
            results.get("lastname"),
            results["id"]
        )
        return results

    def get_stats(self, athlete_id: int) -> dict:
        results = self._get(f"/athletes/{athlete_id}/stats")
        logger.info("Athlete %d — stats fetched", athlete_id)
        return results

    def get_comments(self, activity_id: int) -> list:
        results = self._get(f"/activities/{activity_id}/comments")
        for record in results:
            record["activity_id"] = activity_id
        logger.info("Activity %d — %d comments", activity_id, len(results))
        return results

    def get_kudos(self, activity_id: int) -> list:
        results = self._get(f"/activities/{activity_id}/kudos")
        for record in results:
            record["activity_id"] = activity_id
        logger.info("Activity %d — %d kudos", activity_id, len(results))
        return results


def save_raw(filename: str, data):
    path = os.path.join(RAW_DIR, f"{filename}.json")

    # Dedup key per file type
    dedup_keys = {
        "raw_strava_activities": lambda r: r["id"],
        "raw_strava_comments_partial": lambda r: r["id"],
        "raw_strava_kudos_partial": lambda r: f"{r['activity_id']}_{r['firstname']}_{r['lastname']}"
        }

    if isinstance(data, list) and filename in dedup_keys:
        key_fn = dedup_keys[filename]
        existing = []
        if os.path.exists(path):
            with open(path, "r") as f:
                existing = json.load(f)

        merged = {key_fn(r): r for r in existing}
        merged.update({key_fn(r): r for r in data})
        output = list(merged.values())

    else:
        # dict (athlete, stats) or no dedup key (kudos) — just overwrite
        output = data

    with open(path, "w") as f:
        json.dump(output, f, indent=2)

    logger.info("Saved %s", path)

def run_extraction(con) -> bool:
    """
    Stage 1: Extract from Strava API.
    Derives watermark from bronze table — full load on first run,
    incremental on subsequent runs.
    Returns True if new data was found, False if nothing to process.
    """
    # Derive watermark from bronze — no local file needed
    watermark = None
    try:
        result = con.execute(
            "SELECT MAX(start_date::TIMESTAMP) FROM bronze_activities"
        ).fetchone()[0]

        if result:
            watermark = result
            logger.info("Watermark derived from bronze: %s", watermark)
        else:
            logger.info("Bronze table empty — full load")
    except Exception:
        logger.info("Bronze table not found — full load")

    client = StravaClient()
    activities = client.get_activities(after=watermark)

    if not activities:
        logger.info("No new activities since last run")
        return False

    athlete = client.get_athlete()
    stats = client.get_stats(athlete_id=athlete["id"])

    save_raw("raw_strava_activities", activities)
    save_raw("raw_athlete", athlete)
    save_raw("raw_stats_for_athlete", stats)

    all_comments, all_kudos = [], []

    for i, activity in enumerate(activities):
        activity_id = activity["id"]

        if activity.get("comment_count", 0) > 0:
            all_comments.extend(client.get_comments(activity_id))

        if activity.get("kudos_count", 0) > 0:
            all_kudos.extend(client.get_kudos(activity_id))

        if i % 20 == 0:
            save_raw("raw_strava_comments_partial", all_comments)
            save_raw("raw_strava_kudos_partial", all_kudos)

        time.sleep(5)

    save_raw("raw_strava_comments_partial", all_comments)
    save_raw("raw_strava_kudos_partial", all_kudos)

    logger.info("Extraction complete — %d new activities", len(activities))
    return True