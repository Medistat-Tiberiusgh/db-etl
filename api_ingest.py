"""
Yearly incremental ingest from the Socialstyrelsen API.

Idempotent and re-runnable: it compares the API's latest year against the
database and loads every year that is newer, replacing each year atomically.
A no-op when there is nothing new. Run as a one-shot (`python -m api_ingest`)
or from the weekly scheduler.

Assumes the bulk CSV seed has already populated the stable lookup tables
(regions, genders, age_groups). Only `drugs` grows year to year, so only that
lookup is upserted here.
"""

import logging

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from src.config import load_config
from src.discord_notifier import DiscordNotifier
from src.postgres_loader import PostgresLoader
from src.socialstyrelsen_api import SocialstyrelsenApi
from src.transformer import apply_transforms

logger = logging.getLogger(__name__)


def run_ingest() -> None:
    """Check for new years and ingest them; notify on success or failure."""
    config = load_config()
    api = SocialstyrelsenApi()
    notifier = DiscordNotifier(config.discord_webhook_url)
    engine = create_engine(config.database_uri)

    try:
        database_year = current_max_year(engine)
        latest_year = api.latest_year()
        new_years = list(range(database_year + 1, latest_year + 1))

        if not new_years:
            logger.info("No new data — database and API both at %d", database_year)
            return

        logger.info("New years to ingest: %s (db=%d, api=%d)", new_years, database_year, latest_year)

        drug_names = api.drug_names()
        upsert_drugs(engine, drug_names)
        atc_codes = list(drug_names)

        loader = PostgresLoader(config.database_uri, "prescription_data")
        try:
            for year in new_years:
                rows = ingest_year(api, loader, year, atc_codes)
                notifier.success(
                    f"Loaded **{rows:,}** rows for **{year}**.",
                    fields=[
                        {"name": "Year", "value": str(year), "inline": True},
                        {"name": "Rows", "value": f"{rows:,}", "inline": True},
                    ],
                )
        finally:
            loader.close()
    except Exception as error:
        logger.exception("Ingest failed")
        notifier.error(f"{type(error).__name__}: {error}")
        raise
    finally:
        engine.dispose()


def ingest_year(
    api: SocialstyrelsenApi,
    loader: PostgresLoader,
    year: int,
    atc_codes: list[str],
) -> int:
    """Transform each API chunk and atomically replace the year in the database."""
    chunks = (apply_transforms(chunk) for chunk in api.fetch_year(year, atc_codes))
    return loader.replace_year(year, chunks)


def current_max_year(engine: Engine) -> int:
    """The newest year already in the database, or 0 if it is empty."""
    with engine.connect() as connection:
        latest = connection.execute(text("SELECT MAX(year) FROM prescription_data")).scalar()
    return int(latest) if latest is not None else 0


def upsert_drugs(engine: Engine, name_by_atc: dict[str, str]) -> None:
    """Insert any ATC codes not already in `drugs`; leave existing rows untouched."""
    rows = [{"atc": atc, "name": name} for atc, name in name_by_atc.items()]
    with engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO drugs (atc, name) VALUES (:atc, :name) "
                "ON CONFLICT (atc) DO NOTHING"
            ),
            rows,
        )
    logger.info("Upserted %d drugs (new ATCs inserted, existing left untouched)", len(rows))


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    )
    run_ingest()
