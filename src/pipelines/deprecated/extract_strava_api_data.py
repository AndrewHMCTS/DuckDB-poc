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
    format="%(asctime)s [%(levelname)s] %(message)s", ) 

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

        #define period + 60s buffer to rotate expired token
        expires_at = data["expires_at"]
        self._token_expiry = datetime.fromtimestamp(expires_at, tz=timezone.utc) - timedelta(seconds=60)

        #refresh token if expired
        if "refresh_token" in data:
            os.environ["STRAVA_REFRESH_TOKEN"] = data["refresh_token"]
        logger.info(f"Token refreshed. Expires at {self._token_expiry}")

    ##OAuth 
    def _get_token(self) -> dict: 
        response = requests.post( f"{self.BASE_URL}/oauth/token", 
                                 data={ 
                                     "client_id": os.getenv("STRAVA_CLIENT_ID"), 
                                     "client_secret": os.getenv("STRAVA_CLIENT_SECRET"), 
                                     "refresh_token": os.getenv("STRAVA_REFRESH_TOKEN"), 
                                     "grant_type": "refresh_token", 
                                     } 
                                ) 
        response.raise_for_status() 
        logger.debug("Access token refreshed successfully") 
        
        return response.json()

    ##get request
    def _get(self, path: str, params: dict = None) -> dict:
        response = requests.get(
            f"{self.BASE_URL}{path}",
            headers={"Authorization": f"Bearer {self.token}"},
            params=params or {},
        )
        response.raise_for_status()
        return response.json()

    ##query activity endpoint
    def get_activities(self) -> list:
        activities, page = [], 1
        while True:
            batch = self._get("/athlete/activities", {"per_page": 200, "page": page})
            if not batch:
                break
            activities.extend(batch)
            logger.info("Activities — fetched page %d (%d in batch)", page, len(batch))
            page += 1
        logger.info("Activities — total fetched: %d", len(activities))
        return activities
    
    ##query comments endpoint
    def get_athlete(self) -> dict:
        results = self._get("/athlete")
        logger.info("Athlete %s %s — id: %d", results.get("firstname"), results.get("lastname"), results["id"])
        return results
    
    ##query stats endpoint
    def get_stats(self, athlete_id: int) -> dict:
        results = self._get(f"/athletes/{athlete_id}/stats")
        logger.info("Athlete %d — stats fetched", athlete_id)
        return results

    ##query comments endpoint
    def get_comments(self, activity_id: int) -> list:
        results = self._get(f"/activities/{activity_id}/comments")
        for record in results:
            record["activity_id"] = activity_id
        logger.info("Activity %d — comments: %d", activity_id, len(results))
        return results

    ##query kudos endpoint
    def get_kudos(self, activity_id: int) -> list:
        results = self._get(f"/activities/{activity_id}/kudos")
        for record in results:
            record["activity_id"] = activity_id
        logger.info("Activity %d — kudos: %d", activity_id, len(results))
        return results

##func to save file to .raw/
def save_raw(filename: str, data):
    """Save data to raw/<filename>.json"""
    os.makedirs("raw", exist_ok=True)
    with open(f"raw/{filename}.json", "w") as f:
        json.dump(data, f, indent=2)
    logger.info("Saved raw/%s.json", filename)

##initiate Strava class and extract all activites, athlete info and stats for athlete
client = StravaClient()
activities = client.get_activities()
athlete = client.get_athlete()
stats_for_athlete = client.get_stats(athlete_id=athlete["id"])

save_raw("raw_strava_activities", activities)
save_raw("raw_athlete", athlete)
save_raw("raw_stats_for_athlete", stats_for_athlete)

#store raw json values from endpoint in ./raw
all_comments = []
all_kudos    = []

##per strava API, pull out comments and kudos linked to an activityid
for i, activity in enumerate(activities):
    activity_id = activity["id"]

    if activity.get("comment_count", 0) > 0:
        all_comments.extend(client.get_comments(activity_id))

    if activity.get("kudos_count", 0) > 0:
        all_kudos.extend(client.get_kudos(activity_id))

    # save records every 20 to avoid 429 error disrupting saves
    if i % 20 == 0:
        save_raw("raw_strava_comments_partial", all_comments)
        save_raw("raw_strava_kudos_partial", all_kudos)

    # try to avoid rate limiting
    time.sleep(5)