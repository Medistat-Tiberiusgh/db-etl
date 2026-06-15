"""
ETL pipeline orchestrator.

Reads a CSV in chunks, cleans each chunk, and loads it into Postgres.
"""

import logging

from .extractor import read_csv_chunks
from .postgres_loader import PostgresLoader
from .transformer import apply_transforms

logger = logging.getLogger(__name__)


def run(csv_path: str, chunk_size: int, loader: PostgresLoader) -> int:
    """
    Execute the full ETL pipeline.

    Returns the total number of rows processed.
    """
    total_rows = 0

    for chunk_number, raw_chunk in enumerate(read_csv_chunks(csv_path, chunk_size), start=1):
        transformed = apply_transforms(raw_chunk)
        total_rows += len(transformed)

        loader.load(transformed)
        logger.info("Chunk %d processed (%d rows)", chunk_number, len(transformed))

    logger.info("Pipeline complete — %d total rows processed", total_rows)
    return total_rows
