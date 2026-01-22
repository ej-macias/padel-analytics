import os
import sys
import json
import base64
import gspread
import logging
import pandas as pd

# Configure basic logging
logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
LOGGER = logging.getLogger(__name__)

    
REQUIRED_ENVS = ["GOOGLE_SERVICE_ACCOUNT", "GOOGLE_SHEET_ID_PADEL_STATS"]

def _require_envs(logger: logging.Logger) -> None:
    missing = [v for v in REQUIRED_ENVS if not os.environ.get(v)]
    if missing:
        logger.error("Missing required environment variables: %s", ", ".join(missing))
        raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")


def open_sheet() -> gspread.Spreadsheet:
    """
    Authenticate with Google Service Account and open the Google Sheet.
    """
    _require_envs(LOGGER)

    # Decode the Google Service Account from environment variable
    GOOGLE_SA = os.environ.get("GOOGLE_SERVICE_ACCOUNT")
    sa_decoded = base64.b64decode(GOOGLE_SA, validate=True)
    sa_json = json.loads(sa_decoded)

    # Google Sheets API scope
    SCOPE = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    # Authenticate and create the gspread client
    try:
        gc = gspread.service_account_from_dict(sa_json, scopes=SCOPE)
    except Exception as e:
        LOGGER.error("Failed to authenticate with Google Sheets API: %s", e)
        sys.exit(2)

    LOGGER.info("Authenticated with Google Sheets API successfully.")

    # Open the Google Sheet
    try:
        sheet_id = os.environ["GOOGLE_SHEET_ID_PADEL_STATS"]
        sh = gc.open_by_key(sheet_id)
    except Exception as e:
        LOGGER.error("Failed to open Google Sheet: %s", e)
        sys.exit(2)

    LOGGER.info("Opened Google Sheet successfully.")
    return sh


def export_df_to_sheet(worksheet_name: str, df: pd.DataFrame, append_rows: bool = False) -> None:
    """
    Clear and overwrite an existing worksheet with a DataFrame.
    """
    sh = open_sheet()
    try:
        ws = sh.worksheet(worksheet_name)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=worksheet_name, rows="1000", cols="20")

    # Replace NaN/NaT so Sheets doesn't get "nan"
    df = df.astype(object).where(pd.notna(df), "")

    values = df.values.tolist()
    headers = df.columns.tolist()

    if append_rows and ws.row_count > 0 and ws.get_all_values():
        # Append only data rows (no header)
        ws.append_rows(values, value_input_option="USER_ENTERED")
    else:
        # Overwrite sheet (clear + header + data)
        ws.clear()
        ws.update([headers] + values, value_input_option="USER_ENTERED")

    LOGGER.info(f"Exported {len(df)} rows x {len(df.columns)} cols to sheet '{worksheet_name}'.")
