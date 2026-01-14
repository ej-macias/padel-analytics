import os
import sys
import json
import gspread
from google.oauth2.service_account import Credentials
import logging
import pandas as pd

# Configure basic logging
logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

    
def get_client(worksheet_name: str) -> gspread.Client:
    """
    Get Google sheet ID and Authenticate using a service account JSON from env vars.
    """
    REQUIRED_ENVS = ["GOOGLE_SERVICE_ACCOUNT_JSON"]

    if worksheet_name == "match_summary":
        sheet_id = os.environ["GOOGLE_SHEET_ID_MATCH_SUMMARY"]
        if sheet_id is None:
            logger.error(
                "Missing required environment variable: GOOGLE_SHEET_ID_MATCH_SUMMARY"
            )
            sys.exit(2)
        
    else:
        logger.error("Unknown worksheet name: %s", worksheet_name)
        sys.exit(2)

    sa_json = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
    if sa_json is None:
        logger.error(
            "Missing required environment variable: GOOGLE_SERVICE_ACCOUNT_JSON"
        )
        sys.exit(2)

    # Google Sheets API scope
    SCOPE = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    creds = Credentials.from_service_account_info(
        json.loads(sa_json),
        scopes=SCOPE,
    )
    return gspread.authorize(creds), sheet_id


def write_dataframe(
    worksheet_name: str,
    df,
):
    """
    Clear and overwrite an existing worksheet with a DataFrame.
    """
    gc, sheet_id = get_client()
    sh = gc.open_by_key(sheet_id)
    ws = sh.worksheet(worksheet_name)

    # Replace NaN/NaT so Sheets doesn't get "nan"
    df = df.astype(object).where(pd.notna(df), "")

    # Sheets-friendly values
    df = df.astype(object).where(df.notna(), "")
    values = [df.columns.tolist()] + df.values.tolist()

    ws.clear()
    ws.update(values)

    """
    # Archive previous sheet and create new sheet
    yesterday = pd.Timestamp.now() - pd.Timedelta(days=1)
    ws.update_title(f"{sheet_name}_{yesterday.strftime('%Y%m%d')}")
    sh.add_worksheet(title=sheet_name, rows="1000", cols="20")
    ws = sh.worksheet(worksheet_name)
    ws.update(values)
    """

    logger.info(f"Exported {len(df)} rows x {len(df.columns)} cols to sheet '{worksheet_name}'.")
