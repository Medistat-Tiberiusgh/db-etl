"""
ETL pipeline orchestrator.

Coordinates extraction, transformation, and loading while staying
agnostic to the concrete loader implementation (Dependency Inversion).
"""

import logging
from collections.abc import Callable

import pandas as pd

from .extractor import read_csv_chunks
from .loaders.base import Loader

logger = logging.getLogger(__name__)

TransformFn = Callable[[pd.DataFrame], pd.DataFrame]


def run(
    csv_path: str,
    chunk_size: int,
    loaders: list[Loader],
    transform: TransformFn,
) -> int:
    """
    Execute the full ETL pipeline.

    Returns the total number of rows processed.
    """
    total_rows = 0

    for chunk_number, raw_chunk in enumerate(read_csv_chunks(csv_path, chunk_size), start=1):
        transformed = transform(raw_chunk)
        rows_in_chunk = len(transformed)
        total_rows += rows_in_chunk

        for loader in loaders:
            loader.load(transformed)

        logger.info("Chunk %d processed (%d rows)", chunk_number, rows_in_chunk)

    logger.info("Pipeline complete — %d total rows processed", total_rows)
    return total_rows
