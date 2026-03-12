"""
Entry point for the CSV-to-Database ETL pipeline.

Loads every *.csv file found in DATA_DIR into PostgreSQL,
using the filename (without extension) as the table name.

Usage:
    python main.py
"""

import logging
from pathlib import Path

from src.config import load_config
from src.loaders import SqlLoader
from src.pipeline import run
from src.transformer import apply_transforms


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    )

    log = logging.getLogger(__name__)
    config = load_config()
    csv_files = sorted(Path(config.data_dir).glob("*.csv"), key=lambda p: (p.stem == "prescription_data", p.name))

    if not csv_files:
        log.warning("No CSV files found in %s", config.data_dir)
        return

    for csv_file in csv_files:
        table = csv_file.stem
        log.info("Processing %s → table '%s'", csv_file.name, table)
        loader = SqlLoader(uri=config.sql.uri, table=table)
        try:
            total = run(
                csv_path=str(csv_file),
                chunk_size=config.chunk_size,
                loaders=[loader],
                transform=apply_transforms,
            )
            log.info("Done — %d rows loaded into '%s'", total, table)
        finally:
            loader.close()


if __name__ == "__main__":
    main()
