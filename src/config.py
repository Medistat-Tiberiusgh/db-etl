"""
Configuration loaded from environment variables.

Keeps secrets out of source code and makes the app configurable
across environments (dev, CI, prod) without code changes.
"""

import os
from dataclasses import dataclass, field

from dotenv import load_dotenv


@dataclass(frozen=True)
class Config:
    data_dir: str
    database_uri: str = field(repr=False)
    chunk_size: int = 100_000


def load_config() -> Config:
    """Build configuration from environment variables."""
    load_dotenv()

    database_uri = (
        f"postgresql://{os.environ['DB_USER']}:{os.environ['DB_PASSWORD']}"
        f"@{os.environ['DB_HOST']}:{os.environ['DB_PORT']}/{os.environ['DB_NAME']}"
    )
    return Config(
        data_dir=os.environ.get("DATA_DIR", "/data"),
        database_uri=database_uri,
        chunk_size=int(os.environ.get("CHUNK_SIZE", "100000")),
    )
