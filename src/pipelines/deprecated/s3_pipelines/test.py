import json
import logging
import os 
import requests 
import time
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv 

load_dotenv()

logging.basicConfig( 
    level=logging.INFO, 
    format="%(asctime)s [%(levelname)s] %(message)s") 

logger = logging.getLogger(__name__) 

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
        self._token_expiry = datetime.fromtimestamp(expires_at, tz=timezone.utc) - timedelta(seconds=60)
        if "refresh_token" in data:
            os.environ["STRAVA_REFRESH_TOKEN"] = data["refresh_token"]
        logger.info(f"Token refreshed. Expires at {self._token_expiry}")

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

    def get_activities(self, after: datetime = None) -> list:
        """
        Fetch activities from Strava.
        
        after: only return activities AFTER this datetime (incremental load)
               if None, fetches all activities (full load / first run)
        """
        params = {"per_page": 200}
        
        # Strava expects unix timestamp for after/before params
        if after:
            after_unix = int(after.timestamp())
            params["after"] = after_unix
            logger.info("Incremental load — fetching activities after %s", after.isoformat())
        else:
            logger.info("Full load — fetching all activities")

        activities, page = [], 1
        while True:
            params["page"] = page
            batch = self._get("/athlete/activities", params)
            if not batch:
                break
            activities.extend(batch)
            logger.info("Activities — fetched page %d (%d in batch)", page, len(batch))
            page += 1

        logger.info("Activities — total fetched: %d", len(activities))
        return activities

    def get_athlete(self) -> dict:
        results = self._get("/athlete")
        logger.info("Athlete %s %s — id: %d", results.get("firstname"), results.get("lastname"), results["id"])
        return results

    def get_stats(self, athlete_id: int) -> dict:
        results = self._get(f"/athletes/{athlete_id}/stats")
        logger.info("Athlete %d — stats fetched", athlete_id)
        return results

    def get_comments(self, activity_id: int) -> list:
        results = self._get(f"/activities/{activity_id}/comments")
        for record in results:
            record["activity_id"] = activity_id
        logger.info("Activity %d — comments: %d", activity_id, len(results))
        return results

    def get_kudos(self, activity_id: int) -> list:
        results = self._get(f"/activities/{activity_id}/kudos")
        for record in results:
            record["activity_id"] = activity_id
        logger.info("Activity %d — kudos: %d", activity_id, len(results))
        return results


def save_raw(filename: str, data):
    """Save data to raw/<filename>.json"""
    os.makedirs("raw", exist_ok=True)
    with open(f"raw/{filename}.json", "w") as f:
        json.dump(data, f, indent=2)
    logger.info("Saved raw/%s.json", filename)


def get_watermark(watermark_file: str = "watermark.json") -> datetime | None:
    """
    Read the last successful run timestamp from a local file.
    Returns None if no watermark exists (first run).
    """
    if not os.path.exists(watermark_file):
        logger.info("No watermark found — this is a full load")
        return None
    
    with open(watermark_file) as f:
        data = json.load(f)
    
    watermark = datetime.fromisoformat(data["last_loaded_at"])
    logger.info("Watermark found — last loaded at %s", watermark.isoformat())
    return watermark

def save_watermark(activities: list, watermark_file: str = "watermark.json"):
    """
    Save the most recent activity date as the new watermark.
    Only called after pipeline completes successfully.
    """
    # Strava returns start_date as ISO string e.g. "2024-01-15T08:30:00Z"
    latest = max(a["start_date"] for a in activities)
    
    with open(watermark_file, "w") as f:
        json.dump({"last_loaded_at": latest}, f, indent=2)
    
    logger.info("Watermark saved — next run will load after %s", latest)


# ── Pipeline ────────────────────────────────────────────────────────────────

client = StravaClient()

# Check watermark — determines full load vs incremental
watermark = get_watermark()

# Only pull what we haven't seen before
activities = client.get_activities(after=watermark)

if not activities:
    logger.info("No new activities since last run — exiting")
    exit(0)

# Athlete + stats are lightweight, always refresh
athlete = client.get_athlete()
stats_for_athlete = client.get_stats(athlete_id=athlete["id"])

save_raw("raw_strava_activities", activities)
save_raw("raw_athlete", athlete)
save_raw("raw_stats_for_athlete", stats_for_athlete)

all_comments = []
all_kudos = []

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

save_raw("raw_strava_comments", all_comments)
save_raw("raw_strava_kudos", all_kudos)

# ── Only save watermark if everything above succeeded ───────────────────────
save_watermark(activities)