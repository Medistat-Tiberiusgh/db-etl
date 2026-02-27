"""MongoDB loader using PyMongo."""

import logging

import pandas as pd
from pymongo import MongoClient

from .base import Loader

logger = logging.getLogger(__name__)


class MongoLoader(Loader):
    """Load DataFrames into a MongoDB collection."""

    def __init__(self, uri: str, database: str, collection: str) -> None:
        self._client: MongoClient = MongoClient(uri)
        self._collection = self._client[database][collection]
        self._total = 0

    def load(self, df: pd.DataFrame) -> None:
        df = df.where(pd.notnull(df), None)
        records = df.to_dict(orient="records")
        if not records:
            return
        self._collection.insert_many(records)
        self._total += len(records)
        logger.info("Inserted %d documents into MongoDB (total %d)", len(records), self._total)

    def close(self) -> None:
        self._client.close()
        logger.info("MongoDB connection closed")
