import requests
import os
import pandas as pd

API_URL = "https://padelapi.org/api/players/"
API_TOKEN = os.environ["PADEL_API_TOKEN"]

headers = {
    "Authorization": f"Bearer {API_TOKEN}",
    "Accept": "application/json"
}

params = {
    "limit": 100,
    "offset": 0
}

response = requests.get(API_URL, headers=headers)

try:
    if response.status_code == 200:
        json_data = response.json()["data"]
        df_players = pd.json_normalize(json_data)
        print(f"Player count: {len(df_players)}")
        print(df_players.head())
    else:
        print(f"Status Code: {response.status_code}")
except requests.exceptions.HTTPError as e:
    print(f"Status Code: {response.status_code}")
    print(f"HTTP error occurred: {e}")
