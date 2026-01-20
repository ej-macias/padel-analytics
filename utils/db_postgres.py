import os
import sys
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import logging
from datetime import datetime

def get_engine():
    """
    Create a SQLAlchemy engine for Neon / Postgres.
    Expects NEON_DATABASE_URL env var (with sslmode=require).
    """

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

        # Postgres configuration
    username = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PWD"]
    host = os.environ["POSTGRES_HOST"]
    database = os.environ["POSTGRES_DB"]

    engine_url = f"postgresql+psycopg2://{username}:{password}@{host}/{database}"
    logger.info("Connecting to database %s", database)
    return create_engine(engine_url, pool_pre_ping=True), logger



def get_last_update_date(table_name: str, schema: str):
    """
    Run a SELECT query to find the latest created_at timestamp.
    """
    try:
        engine, logger = get_engine()
        with engine.connect() as conn:
            df = pd.read_sql_query(text(f"SELECT MAX(created_at) FROM {schema}.{table_name}"), conn)
            return df.iloc[0,0]
    except SQLAlchemyError as e:
        logger.exception("DB error while getting the last update time: %s", e)
        raise
    except Exception as e:
        logger.exception("Unexpected error while getting the last update time: %s", e)
        raise
    finally:
        try:
            engine.dispose()
        except Exception:
            pass


def read_db_table(table_name: str, schema: str, from_timestamp=None) -> pd.DataFrame:
    """
    Run a SELECT query and return a pandas DataFrame.
    """
    try:
        engine, logger = get_engine()
        with engine.connect() as conn:

            if from_timestamp is None:
                df = pd.read_sql_query(text(f"SELECT * FROM {schema}.{table_name}"), conn)
            else:
                df = pd.read_sql_query(text(f"SELECT * FROM {schema}.{table_name} WHERE created_at > '{from_timestamp}'"), conn)

            logger.info("Found %d rows in table %s.%s", len(df), schema, table_name)
            return df

    except SQLAlchemyError as e:
        logger.exception("DB error while writing matches: %s", e)
        raise
    except Exception as e:
        logger.exception("Unexpected error while writing to DB: %s", e)
        raise
    finally:
        try:
            engine.dispose()
        except Exception:
            pass


def write_db(df: pd.DataFrame, table_name, schema, if_exists):
    """
    Store a dataframe in Postgres.
    """
    try:
        engine, logger = get_engine()
        with engine.connect() as conn:
            logger.info("Writing DataFrame to table %s.%s (%s)", schema, table_name, if_exists)
            # method='multi' can speed up bulk inserts; adjust chunksize if needed.
            df.to_sql(table_name, conn, schema=schema, if_exists=if_exists, index=False, method="multi")
    except SQLAlchemyError as e:
        logger.exception("DB error while writing matches: %s", e)
        raise
    except Exception as e:
        logger.exception("Unexpected error while writing to DB: %s", e)
        raise
    finally:
        try:
            engine.dispose()
        except Exception:
            pass