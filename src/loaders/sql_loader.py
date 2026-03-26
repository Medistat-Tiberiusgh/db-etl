"""SQL loader using SQLAlchemy (PostgreSQL, MySQL, SQLite, etc.)."""

import io
import logging

import pandas as pd
from sqlalchemy import create_engine, Engine

from .base import Loader

logger = logging.getLogger(__name__)


class SqlLoader(Loader):
    """Load DataFrames into a SQL table via PostgreSQL COPY protocol."""

    def __init__(self, uri: str, table: str) -> None:
        self._engine: Engine = create_engine(uri)
        self._table = table
        self._total = 0

    def load(self, df: pd.DataFrame) -> None:
        rows = len(df)
        buffer = io.StringIO()
        df.to_csv(buffer, index=False, header=False)
        buffer.seek(0)

        columns = ", ".join(df.columns)
        copy_sql = f"COPY {self._table} ({columns}) FROM STDIN WITH (FORMAT CSV)"

        raw_conn = self._engine.raw_connection()
        try:
            with raw_conn.cursor() as cur:
                cur.copy_expert(copy_sql, buffer)
            raw_conn.commit()
        finally:
            raw_conn.close()

        self._total += rows
        logger.info("Copied %d rows into SQL table '%s' (total %d)", rows, self._table, self._total)

    def close(self) -> None:
        self._engine.dispose()
        logger.info("SQL connection pool disposed")
