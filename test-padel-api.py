import requests
import os
import pandas as pd
from sqlalchemy import create_engine, text


# API connection
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


# Posgres configuration
username = os.environ["POSTGRES_USER"]
password = os.environ["POSTGRES_PWD"]
host = os.environ["POSTGRES_HOST"]
database = os.environ["POSTGRES_DB"]
engine = create_engine(f"postgresql+psycopg2://{username}:{password}@{host}/{database}")


# Fech players from Padel API
response = requests.get(API_URL, headers=headers)


# Store in Postgres Neon Database
try:

    if response.status_code == 200:

        json_data = response.json()["data"]
        df_players = pd.json_normalize(json_data)
        #print(f"Player count: {len(df_players)}")
        
        table_name = "players"
        df_players.to_sql(table_name, engine, if_exists="replace", index=False)
        print(f"✅ Data successfully loaded into table '{table_name}' in database '{database}'.")

    else:

        print(f"Status Code: {response.status_code}")

except requests.exceptions.HTTPError as e:

    print(f"HTTP error occurred: {e}")

