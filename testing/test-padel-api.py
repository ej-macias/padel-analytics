import os
import sys
import time
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import logging
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

# Configure basic logging
logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Required environment variables
REQUIRED_ENVS = [
    "PADEL_API_TOKEN",
    "POSTGRES_USER",
    "POSTGRES_PWD",
    "POSTGRES_HOST",
    "POSTGRES_DB",
]

missing = [v for v in REQUIRED_ENVS if not os.environ.get(v)]
if missing:
    logger.error("Missing required environment variables: %s", ", ".join(missing))
    sys.exit(2)  # non-zero so schedulers know it failed

# API connection
API_URL = "https://padelapi.org/api/players/"
API_TOKEN = os.environ["PADEL_API_TOKEN"]

# Postgres configuration
username = os.environ["POSTGRES_USER"]
password = os.environ["POSTGRES_PWD"]
host = os.environ["POSTGRES_HOST"]
database = os.environ["POSTGRES_DB"]

# Optional configuration
MAX_RETRIES = int(os.environ.get("PADEL_MAX_RETRIES", "2"))
REQUEST_TIMEOUT = int(os.environ.get("PADEL_REQUEST_TIMEOUT", "20"))  # seconds

headers = {
    "Authorization": f"Bearer {API_TOKEN}",
    "Accept": "application/json"
}

params = {
    # "limit": 100,
    # "offset": 0
}

# Force retries only for certain response codes
session = requests.Session()
retries = Retry(
    total=MAX_RETRIES,
    status_forcelist=[408, 429, 500, 502, 503, 504]
)
adapter = HTTPAdapter(max_retries=retries)
session.mount("https://", adapter)
session.mount("http://", adapter)

def fetch_players():
    try:
        logger.info("Requesting %s", API_URL)
        resp = session.get(API_URL, headers=headers, params=params, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()  # raises HTTPError for 4xx/5xx
    except requests.exceptions.HTTPError as e:
        # Non-2xx response
        logger.error("HTTP error fetching players: %s (status %s)", e, getattr(e.response, "status_code", ""))
        raise
    except requests.exceptions.RequestException as e:
        logger.error("Network error fetching players: %s", e)
        raise

    try:
        payload = resp.json()
    except ValueError:
        logger.error("Response is not valid JSON")
        raise

    if "data" not in payload:
        logger.error("API response missing 'data' key")
        raise RuntimeError("API response missing 'data'")

    df = pd.json_normalize(payload["data"])
    logger.info("Fetched %d player records", len(df))
    return df

def store_players(df):
    engine_url = f"postgresql+psycopg2://{username}:{password}@{host}/{database}"
    logger.info("Connecting to database %s", database)
    try:
        engine = create_engine(engine_url, pool_pre_ping=True)
        with engine.begin() as conn:
            table_name = "players"
            logger.info("Writing DataFrame to table '%s' (if_exists=replace)", table_name)
            # method='multi' can speed up bulk inserts; adjust chunksize if needed.
            df.to_sql(table_name, conn, if_exists="replace", index=False, method="multi")
    except SQLAlchemyError as e:
        logger.exception("Database error while writing players: %s", e)
        raise
    except Exception as e:
        logger.exception("Unexpected error while writing to DB: %s", e)
        raise
    finally:
        try:
            engine.dispose()
        except Exception:
            pass

def main():
    start = time.time()
    try:
        df_players = fetch_players()
        if df_players.empty:
            logger.warning("No players fetched; not updating database")
        else:
            store_players(df_players)
    except Exception as e:
        logger.error("Script failed: %s", e)
        # non-zero exit so scheduler detects failure
        sys.exit(1)

    elapsed = time.time() - start
    logger.info("Test script completed successfully in %.2f seconds", elapsed)
    sys.exit(0)

if __name__ == "__main__":
    main()