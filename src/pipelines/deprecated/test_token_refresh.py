import requests, os
from dotenv import load_dotenv

load_dotenv()

response = requests.post(
    "https://www.strava.com/api/v3/oauth/token",
    data={
        "client_id":     os.getenv("STRAVA_CLIENT_ID"),
        "client_secret": os.getenv("STRAVA_CLIENT_SECRET"),
        "code":          "552e3c827b5791dcdba033a0b28539a5733640c9",
        "grant_type":    "authorization_code",
    }
)

tokens = response.json()
print(tokens)

#query url, authorise, copy token into code kv above. integrate this into workflow
##https://www.strava.com/oauth/authorize?client_id=YOUR_CLIENT_ID&response_type=code&redirect_uri=http://localhost/exchange_token&approval_prompt=force&scope=activity:read_all

