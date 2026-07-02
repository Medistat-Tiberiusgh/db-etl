# Medistat — db-etl

The ETL pipeline for Medistat, a visualization tool for dispensed medicines in Sweden. It loads the Socialstyrelsen prescription dataset into PostgreSQL and keeps it current as new years are published.

## How the data is loaded

There are two ways into the same fact table:

- **Bulk CSV seed** — a one-shot load of the full CSV export. The files are streamed in chunks, so files larger than available RAM are no problem. The loader scans the data directory and loads each CSV into its own table, so adding a new file requires no code changes.
- **API ingest** — an always-on scheduler that checks the Socialstyrelsen API for newly published years and loads them idempotently, replacing each year atomically. This is a convenience on top of the seed: re-seeding from a freshly downloaded CSV achieves the same result.

The scheduler reports me through Discord: 🟢 online / 🔴 offline when the container starts and stops, and a summary on every ingest or failure.

## Running it

A sample dataset ships in `sample/`. It has the same files as the full dataset but a fraction of the rows, so seeding completes in seconds. Used mainly for tests. For running locally rename it to `data/` before seeding:

```bash
mv sample data
cp .env.example .env
docker network create edge

docker compose up -d              # Postgres + the scheduler
docker compose run --rm seed      # bulk CSV load
```

A manual API ingest can be run one-shot with `docker compose run --rm scheduler -m api_ingest`.

Everything runs inside Docker, so no Python is needed on the host. The CSV files are not baked into the image; the `data/` directory is mounted as a read-only volume.

## Design notes

- **Load-time cleaning is minimal** — the preprocessing scripts do the heavy lifting, so the only fix at load time is converting the Swedish decimal format (`"2,68"` → `2.68`) so PostgreSQL can parse it.
- **One ATC code per API request** — the API silently truncates large multi-ATC result sets, while a single ATC code always comes back complete.
- **Computed all-ages totals** — the API's age dimension has no "all ages" total like the CSV export does, so the ingest synthesises it by summing the age bands, keeping API-ingested years in the same shape as seeded ones.

## Dataset

The data comes from two Swedish public health authorities:

- **Socialstyrelsen** (National Board of Health and Welfare) — prescription statistics from 2006 onward for all human drugs, via the [open statistics API](https://www.socialstyrelsen.se/statistik-och-data/statistik/for-utvecklare/).
- **Läkemedelsverket** (Medical Products Agency) — narcotic classification per ATC code, extracted from the national product register (NPL) and joined into the drugs lookup table.

The scripts in `scripts/` prepare the raw data: `narcotics_extractor.py` builds the ATC-to-narcotic-class mapping from the NPL XML files, and `preprocessing.py` joins drug names and narcotic classes into the raw export and generates the lookup tables.

The result is one fact table with a row per year, region, drug, gender, and age group (dispensings, patients, and dispensings per 1,000 inhabitants), plus four lookup tables for drugs, regions, genders, and age groups.
