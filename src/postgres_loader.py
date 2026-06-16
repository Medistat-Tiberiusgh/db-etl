"""Loads DataFrames into PostgreSQL via the COPY protocol."""

import io
import logging
from collections.abc import Iterable

import pandas as pd
from sqlalchemy import create_engine, Engine

logger = logging.getLogger(__name__)


class PostgresLoader:
    """Bulk-load DataFrames into a Postgres table using COPY FROM STDIN."""

    def __init__(self, uri: str, table: str) -> None:
        self._engine: Engine = create_engine(uri)
        self._table = table
        self._total = 0

    def load(self, df: pd.DataFrame) -> None:
        """COPY a single chunk in its own transaction (used by the bulk CSV seed)."""
        raw_conn = self._engine.raw_connection()
        try:
            with raw_conn.cursor() as cursor:
                rows = self._copy(cursor, df)
            raw_conn.commit()
        finally:
            raw_conn.close()

        self._total += rows
        logger.info("Copied %d rows into '%s' (total %d)", rows, self._table, self._total)

    def replace_year(self, year: int, chunks: Iterable[pd.DataFrame]) -> int:
        """
        Atomically replace one year of data: delete the year, then COPY the new
        chunks, all in a single transaction. Idempotent and re-runnable, which
        is what makes the yearly API ingest safe (and handles source revisions).

        Used by the API path; the bulk CSV seed keeps using load() unchanged.
        Assumes the target table has a `year` column (i.e. prescription_data).
        """
        raw_conn = self._engine.raw_connection()
        inserted = 0
        try:
            with raw_conn.cursor() as cursor:
                cursor.execute(f"DELETE FROM {self._table} WHERE year = %s", (year,))
                deleted = cursor.rowcount
                for chunk in chunks:
                    if not chunk.empty:
                        inserted += self._copy(cursor, chunk)
            raw_conn.commit()
        except Exception:
            raw_conn.rollback()
            raise
        finally:
            raw_conn.close()

        logger.info(
            "Replaced year %d in '%s': deleted %d, inserted %d",
            year, self._table, deleted, inserted,
        )
        return inserted

    def _copy(self, cursor, df: pd.DataFrame) -> int:
        """COPY one DataFrame into the table on an open cursor; returns row count."""
        buffer = io.StringIO()
        df.to_csv(buffer, index=False, header=False)
        buffer.seek(0)

        columns = ", ".join(df.columns)
        cursor.copy_expert(
            f"COPY {self._table} ({columns}) FROM STDIN WITH (FORMAT CSV)",
            buffer,
        )
        return len(df)

    def close(self) -> None:
        self._engine.dispose()
        logger.info("Postgres connection pool disposed")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
