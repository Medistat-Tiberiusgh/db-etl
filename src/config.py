"""
Configuration loaded from environment variables.

Keeps secrets out of source code and makes the app configurable
across environments (dev, CI, prod) without code changes.
"""

import os
from dataclasses import dataclass, field

from dotenv import load_dotenv


@dataclass(frozen=True)
class SqlConfig:
    uri: str = field(repr=False)


@dataclass(frozen=True)
class EtlConfig:
    data_dir: str
    chunk_size: int = 100_000
    sql: SqlConfig = field(default_factory=lambda: SqlConfig(uri=""))


def load_config() -> EtlConfig:
    """Build configuration from environment variables."""
    load_dotenv()

    sql_uri = (
        f"postgresql://{os.environ['DB_USER']}:{os.environ['DB_PASSWORD']}"
        f"@{os.environ['DB_HOST']}:{os.environ['DB_PORT']}/{os.environ['DB_NAME']}"
    )
    return EtlConfig(
        data_dir=os.environ.get("DATA_DIR", "/data"),
        chunk_size=int(os.environ.get("CHUNK_SIZE", "100000")),
        sql=SqlConfig(uri=sql_uri),
    )
