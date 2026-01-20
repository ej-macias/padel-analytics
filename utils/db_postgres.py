import os
import logging
from functools import lru_cache
from contextlib import contextmanager
from typing import Iterator, Optional

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, Connection
from sqlalchemy.exc import SQLAlchemyError


LOGGER = logging.getLogger("db")
if not LOGGER.handlers:  # prevents duplicate handlers if imported multiple times
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

REQUIRED_ENVS = ["POSTGRES_USER", "POSTGRES_PWD", "POSTGRES_HOST", "POSTGRES_DB"]

def _require_envs(logger: logging.Logger) -> None:
    missing = [v for v in REQUIRED_ENVS if not os.environ.get(v)]
    if missing:
        logger.error("Missing required environment variables: %s", ", ".join(missing))
        raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")


@lru_cache(maxsize=1)
def get_engine() -> Engine:
    """
    Create and cache a SQLAlchemy engine (reused across calls).
    Safe to call many times; returns the same Engine instance.
    """
    _require_envs(LOGGER)

    username = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PWD"]
    host = os.environ["POSTGRES_HOST"]
    database = os.environ["POSTGRES_DB"]

    engine_url = f"postgresql+psycopg2://{username}:{password}@{host}/{database}"
    LOGGER.info("Creating DB engine for %s", database)

    return create_engine(
        engine_url,
        pool_pre_ping=True,
        pool_size=int(os.environ.get("DB_POOL_SIZE", "5")),
        max_overflow=int(os.environ.get("DB_MAX_OVERFLOW", "5")),
    )


@contextmanager
def db_conn(op_name: str) -> Iterator[Connection]:
    """
    Yields a Connection, with centralized error logging.
    """
    engine = get_engine()
    try:
        with engine.connect() as conn:
            yield conn
    except SQLAlchemyError as e:
        LOGGER.exception("DB error during %s: %s", op_name, e)
        raise
    except Exception as e:
        LOGGER.exception("Unexpected error during %s: %s", op_name, e)
        raise


def get_last_update_date(table_name: str, schema: str):
    with db_conn("get_last_update_date") as conn:
        df = pd.read_sql_query(
            text(f"SELECT MAX(created_at) AS max_created_at FROM {schema}.{table_name}"),
            conn,
        )
        return df.iloc[0, 0]


def read_db_table(table_name: str, schema: str, from_timestamp: Optional[str] = None) -> pd.DataFrame:
    with db_conn("read_db_table") as conn:
        if from_timestamp is None:
            q = text(f"SELECT * FROM {schema}.{table_name}")
            params = {}
        else:
            # Use bind params instead of string interpolation
            q = text(f"SELECT * FROM {schema}.{table_name} WHERE created_at > :from_ts")
            params = {"from_ts": from_timestamp}

        df = pd.read_sql_query(q, conn, params=params)
        LOGGER.info("Found %d rows in table %s.%s", len(df), schema, table_name)
        return df


def write_db(df: pd.DataFrame, table_name: str, schema: str, if_exists: str) -> None:
    with db_conn("write_db") as conn:
        LOGGER.info("Writing DataFrame to table %s.%s (%s)", schema, table_name, if_exists)
        df.to_sql(
            table_name,
            conn,
            schema=schema,
            if_exists=if_exists,
            index=False,
            method="multi",
        )
