"""
Yearly incremental ingest from the Socialstyrelsen API.

Idempotent and re-runnable: it compares the API's latest year against the
database and loads every year that is newer, replacing each year atomically.
Notifies Discord with a detailed report on every outcome — new data ingested,
nothing new, or a failure. Run as a one-shot (`python -m api_ingest`) or from
the weekly scheduler.

Assumes the bulk CSV seed has already populated the stable lookup tables
(regions, genders, age_groups). Only `drugs` grows year to year, so only that
lookup is upserted here.
"""

import logging
import time

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from src.config import load_config
from src.discord_notifier import DiscordNotifier
from src.postgres_loader import PostgresLoader, YearLoad
from src.socialstyrelsen_api import SocialstyrelsenApi
from src.transformer import apply_transforms

logger = logging.getLogger(__name__)


def run_ingest() -> None:
    """Check for new years, ingest them, and report the outcome to Discord."""
    config = load_config()
    api = SocialstyrelsenApi()
    notifier = DiscordNotifier(config.discord_webhook_url)
    engine = create_engine(config.database_uri)

    run_started = time.monotonic()
    phase = "checking the API"
    year_in_progress: int | None = None

    try:
        database_year = current_max_year(engine)
        latest_year = api.latest_year()
        new_years = list(range(database_year + 1, latest_year + 1))

        if not new_years:
            logger.info("No new data — database and API both at %d", database_year)
            minimum_year, maximum_year = year_span(engine)
            notifier.info(
                f"No new data — database and API are both at **{database_year}**.",
                fields=[
                    {"name": "Database year", "value": str(database_year), "inline": True},
                    {"name": "Latest API year", "value": str(latest_year), "inline": True},
                    {"name": "Coverage", "value": f"{minimum_year}–{maximum_year}", "inline": True},
                    {"name": "Prescription rows", "value": f"{count_rows(engine):,}", "inline": True},
                    {"name": "Drugs", "value": f"{count_drugs(engine):,}", "inline": True},
                ],
            )
            return

        logger.info("New years to ingest: %s (db=%d, api=%d)", new_years, database_year, latest_year)

        phase = "upserting drugs"
        drug_names = api.drug_names()
        new_drugs = upsert_drugs(engine, drug_names)
        atc_codes = list(drug_names)

        loads: list[tuple[int, YearLoad, float]] = []
        loader = PostgresLoader(config.database_uri, "prescription_data")
        try:
            for year in new_years:
                year_in_progress = year
                phase = f"loading year {year}"
                started = time.monotonic()
                load = ingest_year(api, loader, year, atc_codes)
                loads.append((year, load, time.monotonic() - started))
        finally:
            loader.close()

        notifier.success(
            f"Ingested **{len(new_years)}** new year(s) from the Socialstyrelsen API "
            f"in {format_duration(time.monotonic() - run_started)}.",
            fields=_success_fields(engine, new_years, new_drugs, loads),
        )
    except Exception as error:
        logger.exception("Ingest failed")
        notifier.error(
            f"Failed while **{phase}**.\n```\n{type(error).__name__}: {error}\n```"[:1900],
            fields=[
                {"name": "Phase", "value": phase, "inline": True},
                {"name": "Year in progress", "value": str(year_in_progress or "—"), "inline": True},
                {"name": "Error", "value": type(error).__name__, "inline": True},
                {"name": "DB year (intact)", "value": safe_read(lambda: str(current_max_year(engine))), "inline": True},
                {"name": "Rows (intact)", "value": safe_read(lambda: f"{count_rows(engine):,}"), "inline": True},
                {"name": "Elapsed", "value": format_duration(time.monotonic() - run_started), "inline": True},
            ],
        )
        raise
    finally:
        engine.dispose()


def _success_fields(
    engine: Engine,
    new_years: list[int],
    new_drugs: int,
    loads: list[tuple[int, YearLoad, float]],
) -> list[dict]:
    """Build the detailed field list for a successful ingest report."""
    inserted = sum(load.inserted for _, load, _ in loads)
    deleted = sum(load.deleted for _, load, _ in loads)
    minimum_year, maximum_year = year_span(engine)
    breakdown = "\n".join(
        f"{year}: +{load.inserted:,} (replaced {load.deleted:,}, {format_duration(seconds)})"
        for year, load, seconds in loads
    )
    return [
        {"name": "Years", "value": ", ".join(str(year) for year in new_years), "inline": True},
        {"name": "Rows inserted", "value": f"{inserted:,}", "inline": True},
        {"name": "Rows replaced", "value": f"{deleted:,}", "inline": True},
        {"name": "New drugs", "value": f"{new_drugs:,}", "inline": True},
        {"name": "Coverage", "value": f"{minimum_year}–{maximum_year}", "inline": True},
        {"name": "Total rows now", "value": f"{count_rows(engine):,}", "inline": True},
        {"name": "Drugs now", "value": f"{count_drugs(engine):,}", "inline": True},
        {"name": "Per year", "value": breakdown, "inline": False},
    ]


def ingest_year(
    api: SocialstyrelsenApi,
    loader: PostgresLoader,
    year: int,
    atc_codes: list[str],
) -> YearLoad:
    """Transform each API chunk and atomically replace the year in the database."""
    chunks = (apply_transforms(chunk) for chunk in api.fetch_year(year, atc_codes))
    return loader.replace_year(year, chunks)


def upsert_drugs(engine: Engine, name_by_atc: dict[str, str]) -> int:
    """Insert any ATC codes not already in `drugs`; returns how many were new."""
    rows = [{"atc": atc, "name": name} for atc, name in name_by_atc.items()]
    with engine.begin() as connection:
        before = connection.execute(text("SELECT count(*) FROM drugs")).scalar_one()
        connection.execute(
            text(
                "INSERT INTO drugs (atc, name) VALUES (:atc, :name) "
                "ON CONFLICT (atc) DO NOTHING"
            ),
            rows,
        )
        after = connection.execute(text("SELECT count(*) FROM drugs")).scalar_one()
    new_drugs = after - before
    logger.info("Upserted drugs: %d new (of %d candidates)", new_drugs, len(rows))
    return new_drugs


def current_max_year(engine: Engine) -> int:
    """The newest year already in the database, or 0 if it is empty."""
    with engine.connect() as connection:
        latest = connection.execute(text("SELECT MAX(year) FROM prescription_data")).scalar()
    return int(latest) if latest is not None else 0


def year_span(engine: Engine) -> tuple[int, int]:
    """The (oldest, newest) year present in prescription_data, or (0, 0) if empty."""
    with engine.connect() as connection:
        oldest, newest = connection.execute(
            text("SELECT MIN(year), MAX(year) FROM prescription_data")
        ).one()
    return (oldest or 0, newest or 0)


def count_rows(engine: Engine) -> int:
    with engine.connect() as connection:
        return connection.execute(text("SELECT count(*) FROM prescription_data")).scalar_one()


def count_drugs(engine: Engine) -> int:
    with engine.connect() as connection:
        return connection.execute(text("SELECT count(*) FROM drugs")).scalar_one()


def format_duration(seconds: float) -> str:
    minutes, secs = divmod(int(seconds), 60)
    return f"{minutes}m {secs}s" if minutes else f"{secs}s"


def safe_read(read) -> str:
    """Run a DB read for an error report; never let it mask the original failure."""
    try:
        return read()
    except Exception:
        return "unknown"


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    )
    run_ingest()
