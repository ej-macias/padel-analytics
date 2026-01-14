import os
import sys
import pandas as pd
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

# API connection
API_URL_BASE = "https://padelapi.org/api/"
API_TOKEN = os.environ["PADEL_API_TOKEN"]

# Optional configuration
MAX_RETRIES = int(os.environ.get("PADEL_MAX_RETRIES", "2"))
REQUEST_TIMEOUT = int(os.environ.get("PADEL_REQUEST_TIMEOUT", "20"))  # seconds
INCREMENTAL_MATCHES = int(os.environ.get("INCREMENTAL_MATCHES", "0"))  # default to False

HEADERS = {
    "Authorization": f"Bearer {API_TOKEN}",
    "Accept": "application/json"
}

# Only matches from yesterday
if INCREMENTAL_MATCHES == 1:
    yesterday = (pd.Timestamp("today") - pd.Timedelta(1, "D")).date()
    params = {
        "before_date": yesterday,
        "after_date": yesterday
    }
else:
    params = {}


def get_session():
    """
    Create and return a configured requests Session for the Padel API.
    Sets up retries, headers, and optional incremental match fetching.
    """

    # Force retries only for certain response codes
    session = requests.Session()
    retries = Retry(
        total=MAX_RETRIES,
        status_forcelist=[408, 429, 500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def api_get(endpoint: str) -> str:
    """
    Run a SELECT query and return a pandas DataFrame.
    """
    try:
        session = get_session()

        api_url = API_URL_BASE + endpoint + "/"
        logger.info("Requesting %s", api_url)
        
        resp = session.get(api_url, headers=HEADERS, params=params, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()  # raises HTTPError for 4xx/5xx

    except requests.exceptions.HTTPError as e:
        # Non-2xx response
        logger.error("HTTP error fetching %s: %s (status %s)", endpoint, e, getattr(e.response, "status_code", ""))
        raise

    except requests.exceptions.RequestException as e:
        logger.error("Network error fetching %s: %s", endpoint, e)
        raise

    try:
        payload = resp.json()
    except ValueError:
        logger.error("Response is not valid JSON")
        raise

    return payload
