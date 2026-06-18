# From CSV to Database — ETL Pipeline

A teaching example that loads the Swedish prescription statistics dataset (five related CSV files) into **PostgreSQL**, structured around clean code and SOLID principles. The pipeline scans a directory for CSV files and loads each one into its own table, using the filename as the name.

## Local development

### Data

A sample dataset is provided in the `sample/` folder. It contains the same five CSV files as the full dataset but with a fraction of the rows, so seeding completes in seconds. This is sufficient to run the API and the test suite.

Rename it to `data/` before seeding:

```bash
mv sample data
```

```bash
# 1. Configure (copy and edit)
cp .env.example .env

# 2. Create the external Docker network (once)
docker network create edge

# 3. Start Postgres + the weekly scheduler
docker compose up -d

# 4. Seed the database (bulk CSV load)
docker compose run --rm seed
```

### Commands

| Command                                           | Role                                            |
| ------------------------------------------------- | ----------------------------------------------- |
| `docker compose up -d`                            | Start Postgres + the always-on weekly scheduler |
| `docker compose run --rm seed`                    | One-shot bulk CSV load (`main.py`)              |
| `docker compose run --rm scheduler -m api_ingest` | One-shot manual API ingest                      |
| `docker compose down`                             | Stop everything                                 |

The scheduler service has `restart: unless-stopped`, so it comes back automatically after a reboot — as long as Docker starts on boot (on macOS, enable "Start Docker Desktop when you sign in"). No need to re-run anything.

## Production / VPS deployment

Everything runs inside Docker — no Python or uv needed on the host.
The CSV files are **not** baked into the Docker image. Instead, the `data/` directory on the host is mounted into the container as a read-only volume.

## How the data is loaded

Socialstyrelsen publishes a full additional year of statistics roughly once a year. There are two ways to keep the database current; both write to the same `prescription_data` table:

- **Bulk CSV seed** (`docker compose run --rm seed`) — the CSV export is all-or-nothing: it always contains _every_ available year (you can't request a single one), so this loads the complete history. Re-running it from a freshly downloaded CSV is a self-sufficient way to update — no API involved.
- **Weekly API ingest** (the always-on `scheduler`, or a `-m api_ingest` one-shot) — a convenience on top of the seed: rather than re-downloading and re-seeding the whole CSV when a new year is published, it pulls just that new year from the Socialstyrelsen API and adds it idempotently (`replace_year`). Entirely optional — re-seeding from a fresh CSV achieves the same result.

If `DISCORD_WEBHOOK_URL` is set, the scheduler reports to Discord: 🟢 online / 🔴 offline when the container starts/stops, and a summary on every ingest or failure. The routine "nothing new" check stays silent.

## Key design decisions

- **Chunked reading** — each CSV is streamed in configurable chunks so files larger than available RAM can be processed.
- **Context-manager support** — `PostgresLoader` implements `__enter__`/`__exit__` so the connection pool is disposed automatically.
- **Targeted load-time cleaning** — the source CSVs are already filtered and renamed by the preprocessing script, so the only fix needed at load time is converting the Swedish decimal format in `per_1000` (`"2,68"` → `2.68`) so PostgreSQL `COPY` can parse it into a `NUMERIC` column.
- **Environment-based config** — secrets stay out of source code; different environments just set different env vars.
- **Directory-based loading** — the pipeline scans `DATA_DIR` for all `.csv` files and loads each into its own table, so adding a new file requires no code changes.
- **Single compose file** — `docker-compose.yml` covers both local dev and deployment using the same `seed` service via `docker compose run`.
- **One ATC per API request** — the yearly API ingest fetches a single ATC code per request. The API silently truncates large multi-ATC result sets (pagination stops early, returning ~58% of the rows), whereas a single ATC is at most 1,188 rows and always comes back complete.
- **Computed all-ages totals** — the API's age dimension only has bands 1–18; it has no "Totalt" (age_group = 99) like the bulk CSV export does. The ingest synthesises it (`add_age_totals`) by summing the bands for counts and re-deriving `per_1000` from the implied population, so API-ingested years match the seeded years' shape.

## Dataset

### Source

The data originates from two Swedish public health authorities:

- **Socialstyrelsen** (National Board of Health and Welfare) — prescription statistics from 2006 onward for all human drugs (the bulk CSV export covers 2006–2024; the weekly API ingest adds each newer year), available via the open statistics API at [socialstyrelsen.se/statistik-och-data/statistik/for-utvecklare](https://www.socialstyrelsen.se/statistik-och-data/statistik/for-utvecklare/) (CSV Statistikdatabasen – Läkemedel 2006–2024). Approximately 46 million rows, partitioned by year, region, gender, and age group.
- **Läkemedelsverket** (Medical Products Agency) — narcotic classification per ATC code, extracted from the Nationellt produktregister för läkemedel (NPL), and joined into the drugs lookup table.

### Preprocessing

The scripts in `scripts/` prepare the raw data for loading:

1. `narcotics_extractor.py` — parses NPL XML product files and builds a mapping of ATC codes to narcotic class (II–V).
2. `preprocessing.py` — joins drug names and narcotic classification into the raw Socialstyrelsen export and generates the four lookup tables.

### Entities

| File                    | Rows  | API role                    | Description                                                                       |
| ----------------------- | ----- | --------------------------- | --------------------------------------------------------------------------------- |
| `prescription_data.csv` | ~46 M | **Primary resource (CRUD)** | Main fact table — one row per (year, region, drug, gender, age group) combination |
| `drugs.csv`             | 1999  | Read-only resource          | All human drugs with ATC code, Swedish name, and narcotic class (if applicable)   |
| `regions.csv`           | 22    | Read-only resource          | Swedish regions (counties + national total "Riket")                               |
| `genders.csv`           | 3     | Read-only resource          | Gender categories (Män / Kvinnor / Båda könen)                                    |
| `age_groups.csv`        | 19    | Read-only resource          | Five-year age bands (0–4, 5–9, … 85+) plus total                                  |

### Key fields — `prescription_data.csv`

| Field               | Type   | Description                             |
| ------------------- | ------ | --------------------------------------- |
| `year`              | int    | Calendar year (2006 onward)             |
| `region`            | int    | Region ID (FK → `regions.id`)           |
| `atc`               | string | 7-character ATC code (FK → `drugs.atc`) |
| `gender`            | int    | Gender ID (FK → `genders.id`)           |
| `age_group`         | int    | Age group ID (FK → `age_groups.id`)     |
| `num_prescriptions` | int    | Number of dispensed prescriptions       |
| `num_patients`      | int    | Number of unique patients               |
| `per_1000`          | float  | Dispensations per 1,000 inhabitants     |
