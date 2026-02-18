"""
CSV extraction with chunked reading for large files.

Yields DataFrames one chunk at a time so the full dataset
never has to fit in memory.
"""

from collections.abc import Iterator
from pathlib import Path

import pandas as pd


def read_csv_chunks(path: str | Path, chunk_size: int) -> Iterator[pd.DataFrame]:
    """Yield successive DataFrames of *chunk_size* rows from a CSV file."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"CSV file not found: {path}")

    yield from pd.read_csv(path, chunksize=chunk_size)
