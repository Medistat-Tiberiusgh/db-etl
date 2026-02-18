"""
Entry point for the CSV-to-Database ETL pipeline.

Usage:
    # Load to both SQL and MongoDB (default)
    python main.py

    # Load to a single target
    python main.py --target sql
    python main.py --target mongo
"""

import argparse
import logging
import sys

from src.config import load_config
from src.loaders import MongoLoader, SqlLoader, Loader
from src.pipeline import run
from src.transformer import apply_transforms


def _build_loaders(target: str, config) -> list[Loader]:
    """Instantiate the requested loader(s) based on the CLI target flag."""
    factories = {
        "sql": lambda: SqlLoader(uri=config.sql.uri, table=config.sql.table),
        "mongo": lambda: MongoLoader(
            uri=config.mongo.uri,
            database=config.mongo.database,
            collection=config.mongo.collection,
        ),
    }

    if target == "all":
        return [factory() for factory in factories.values()]

    if target not in factories:
        raise ValueError(f"Unknown target '{target}'. Choose from: {', '.join(factories)}")

    return [factories[target]()]


def main() -> None:
    parser = argparse.ArgumentParser(description="ETL: CSV → Database")
    parser.add_argument(
        "--target",
        choices=["sql", "mongo", "all"],
        default="all",
        help="Database target (default: all)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    )

    config = load_config()
    loaders = _build_loaders(args.target, config)

    try:
        total = run(
            csv_path=config.csv_path,
            chunk_size=config.chunk_size,
            loaders=loaders,
            transform=apply_transforms,
        )
        logging.getLogger(__name__).info("Done — %d rows loaded to %s", total, args.target)
    finally:
        for loader in loaders:
            loader.close()


if __name__ == "__main__":
    main()
