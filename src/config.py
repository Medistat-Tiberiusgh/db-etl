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
    table: str = "customers"


@dataclass(frozen=True)
class MongoConfig:
    uri: str = field(repr=False)
    database: str = "etl_demo"
    collection: str = "customers"


@dataclass(frozen=True)
class EtlConfig:
    csv_path: str
    chunk_size: int = 5_000
    sql: SqlConfig = field(default_factory=lambda: SqlConfig(uri=""))
    mongo: MongoConfig = field(default_factory=lambda: MongoConfig(uri=""))


def load_config() -> EtlConfig:
    """Build configuration from environment variables."""
    load_dotenv()

    return EtlConfig(
        csv_path=f"/data/{os.environ['CSV_FILENAME']}",
        chunk_size=int(os.environ.get("CHUNK_SIZE", "5000")),
        sql=SqlConfig(
            uri=os.environ.get("SQL_URI", "postgresql://etl_user:etl_pass@localhost:5432/etl_demo"),
            table=os.environ.get("SQL_TABLE", "customers"),
        ),
        mongo=MongoConfig(
            uri=os.environ.get("MONGO_URI", "mongodb://localhost:27017/"),
            database=os.environ.get("MONGO_DB", "etl_demo"),
            collection=os.environ.get("MONGO_COLLECTION", "customers"),
        ),
    )
