"""
Entry point for the CSV-to-Database ETL pipeline.

Loads every *.csv file found in DATA_DIR into its own table,
using the filename (without extension) as the table name.

Usage:
    python main.py
    python main.py --target sql
"""

import argparse
import logging
from pathlib import Path

from src.config import load_config
from src.loaders import MongoLoader, SqlLoader, Loader
from src.pipeline import run
from src.transformer import apply_transforms


def _build_loaders(target: str, config, table: str) -> list[Loader]:
    """Instantiate the requested loader(s) for a single CSV file."""
    factories = {
        "sql": lambda: SqlLoader(uri=config.sql.uri, table=table),
        "mongo": lambda: MongoLoader(
            uri=config.mongo.uri,
            database=config.mongo.database,
            collection=table,
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

    log = logging.getLogger(__name__)
    config = load_config()
    csv_files = sorted(Path(config.data_dir).glob("*.csv"))

    if not csv_files:
        log.warning("No CSV files found in %s", config.data_dir)
        return

    for csv_file in csv_files:
        table = csv_file.stem
        log.info("Processing %s → table '%s'", csv_file.name, table)
        loaders = _build_loaders(args.target, config, table)
        try:
            total = run(
                csv_path=str(csv_file),
                chunk_size=config.chunk_size,
                loaders=loaders,
                transform=apply_transforms,
            )
            log.info("Done — %d rows loaded into '%s'", total, table)
        finally:
            for loader in loaders:
                loader.close()


if __name__ == "__main__":
    main()
