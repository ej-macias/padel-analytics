import os
import sys
import time
import pandas as pd
import logging
from utils.db_postgres import read_db_table, write_db


# Configure basic logging
logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Required environment variables
REQUIRED_ENVS = [
    "POSTGRES_USER",
    "POSTGRES_PWD",
    "POSTGRES_HOST",
    "POSTGRES_DB",
]

missing = [v for v in REQUIRED_ENVS if not os.environ.get(v)]
if missing:
    logger.error("Missing required environment variables: %s", ", ".join(missing))
    sys.exit(2)  # non-zero so schedulers know it failed

# Optional configuration
INCREMENTAL_MATCHES = int(os.environ.get("INCREMENTAL_MATCHES", "0"))  # default to False (full refresh)

# Postgres configuration
username = os.environ["POSTGRES_USER"]
password = os.environ["POSTGRES_PWD"]
host = os.environ["POSTGRES_HOST"]
database = os.environ["POSTGRES_DB"]

def get_bronze_data(table_name):
    df = read_db_table(table_name, schema="bronze")
    return df


def transform_matches(df_raw_data):
    df_matches = df_raw_data
    df_matches["duration_minutes"] = (
        df_raw_data["duration"]
          .str.split(":", expand=True)
          .astype(float)
          .pipe(lambda x: x[0] * 60 + x[1])
    )
    df_matches["created_at"] = pd.Timestamp("now")
    return df_matches.drop(columns=["duration"])


def transform_scores(df_raw_data):
    df_scores = df_raw_data  # Placeholder for actual transformation
    return df_scores


def store_silver_data(df, table_name):
    if_exists = "append" if INCREMENTAL_MATCHES == 1 else "replace"
    write_db(df, table_name, schema="silver", if_exists=if_exists)


def main():
    start = time.time()
    try:
        df_raw_matches = get_bronze_data(table_name="fact_match")

        if df_raw_matches.empty:
            logger.warning("No matches found in bronze layer.")
        else:
            df_trx_matches = transform_matches(df_raw_matches)
            store_silver_data(df_trx_matches, table_name="fact_match")

        df_raw_scores = get_bronze_data(table_name="fact_point")

        if df_raw_scores.empty:
            logger.warning("No point scores found in bronze layer.")
        else:
            df_trx_scores = transform_scores(df_raw_scores)
            store_silver_data(df_trx_scores, table_name="fact_point")

    except Exception as e:
        logger.error("Match transformation script failed: %s", e)
        # non-zero exit so scheduler detects failure
        sys.exit(1)

    elapsed = time.time() - start
    logger.info("Match transformation script completed successfully in %.2f seconds", elapsed)
    sys.exit(0)

if __name__ == "__main__":
    main()