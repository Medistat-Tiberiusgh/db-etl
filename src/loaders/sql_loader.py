"""SQL loader using SQLAlchemy (PostgreSQL, MySQL, SQLite, etc.)."""

import logging

import pandas as pd
from sqlalchemy import create_engine, Engine

from .base import Loader

logger = logging.getLogger(__name__)


class SqlLoader(Loader):
    """Load DataFrames into a SQL table via SQLAlchemy."""

    def __init__(self, uri: str, table: str) -> None:
        self._engine: Engine = create_engine(uri)
        self._table = table
        self._total = 0

    def load(self, df: pd.DataFrame) -> None:
        rows = len(df)
        df.to_sql(
            self._table,
            self._engine,
            if_exists="append",
            index=False,
            chunksize=1_000,
        )
        self._total += rows
        logger.info("Inserted %d rows into SQL table '%s' (total %d)", rows, self._table, self._total)

    def close(self) -> None:
        self._engine.dispose()
        logger.info("SQL connection pool disposed")
