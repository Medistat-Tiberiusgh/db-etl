# From CSV to Database — ETL Pipeline

A teaching example that loads the Swedish narcotic prescription dataset (five related CSV files) into **PostgreSQL** and/or **MongoDB**, structured around clean code and SOLID principles. The pipeline scans a directory for CSV files and loads each one into its own table or collection, using the filename as the name.

## Project structure

```
.
├── main.py                   # CLI entry point
├── Dockerfile                # Container image for the ETL app
├── docker-compose.yml        # Databases + seed service (profiles)
├── data/                     # ⬅ Place the five CSV files here (gitignored)
├── src/
│   ├── config.py             # Environment-based configuration
│   ├── extractor.py          # Chunked CSV reading (Extract)
│   ├── transformer.py        # Data cleaning functions (Transform)
│   ├── pipeline.py           # ETL orchestrator
│   └── loaders/
│       ├── base.py           # Abstract Loader interface
│       ├── sql_loader.py     # SQLAlchemy implementation
│       └── mongo_loader.py   # PyMongo implementation
├── scripts/
│   ├── narcotics_extractor.py  # Maps ATC codes to narcotic classes from NPL XML
│   ├── preprocessing.py        # Filters raw Socialstyrelsen data → five CSV files
│   └── generate_sample_data.py # Generates synthetic data for local development
├── pyproject.toml
└── .env.example
```

## SOLID principles in practice

| Principle | Where it shows up |
|---|---|
| **Single Responsibility** | Each module handles exactly one concern (extract, transform, or load). |
| **Open / Closed** | Adding a new database target means creating a new `Loader` subclass — no existing code changes. |
| **Liskov Substitution** | `SqlLoader` and `MongoLoader` are interchangeable wherever `Loader` is expected. |
| **Interface Segregation** | The `Loader` base class exposes only `load()` and `close()` — nothing more. |
| **Dependency Inversion** | `pipeline.run()` accepts `list[Loader]`, never importing a concrete database client. |

## Prerequisites

- [uv](https://docs.astral.sh/uv/) (Python package manager)
- [Docker](https://docs.docker.com/get-docker/) and Docker Compose

## Data files

Place all five CSV files in the `data/` directory before running the pipeline:

```
data/
├── prescription_data.csv   # Main fact table (~1.88 M rows)
├── drugs.csv               # Narcotic drug lookup (79 rows)
├── regions.csv             # Swedish region lookup (22 rows)
├── genders.csv             # Gender lookup (2 rows)
└── age_groups.csv          # Age group lookup (19 rows)
```

These files are produced by the preprocessing scripts in `scripts/` (see **Dataset** below) and are gitignored — do not commit them to the repository.

The pipeline loads every `.csv` file it finds in `DATA_DIR` and uses the filename (without extension) as the table or collection name. Update `DATA_DIR` in `.env` if your files are in a different location (default: `data`).

## Local development

Run the Python pipeline on your machine while the databases run in Docker.

```bash
# 1. Install dependencies (creates .venv automatically)
uv sync

# 2. Start the databases
docker compose up -d

# 3. Configure (copy and edit)
cp .env.example .env

# 4. Seed the databases
uv run python main.py               # load to both SQL and MongoDB
uv run python main.py --target sql   # SQL only
uv run python main.py --target mongo # MongoDB only

# Stop the databases when done
docker compose down            # keeps data in volumes
docker compose down -v         # removes data too
```

## Production / VPS deployment

Everything runs inside Docker — no Python or uv needed on the host.
The CSV files are **not** baked into the Docker image. Instead, the `data/` directory on the host is mounted into the container as a read-only volume.

### 1. Upload the data files

The CSV files need to end up in the `data/` directory on the server.

**Option A — Copy from your local machine via SSH:**

```bash
# Make sure the data/ directory exists on the server
ssh user@your-server "mkdir -p ~/from-csv-to-database/data"

# Copy all five files
scp /path/to/data/*.csv user@your-server:~/from-csv-to-database/data/
```

If you're using an SSH key (`.pem` file) instead of a password:

```bash
scp -i ~/.ssh/your-key.pem /path/to/data/*.csv user@your-server:~/from-csv-to-database/data/
```

**Option B — Download directly on the server from a remote URL:**

```bash
ssh user@your-server
cd ~/from-csv-to-database
mkdir -p data
curl -o data/prescription_data.csv https://example.com/prescription_data.csv
# repeat for the other four files
```

### 2. Start the databases

```bash
docker compose up -d
```

This starts **PostgreSQL** and **MongoDB** with health checks. They stay running in the background.

### 3. Seed the databases (run once)

```bash
docker compose run --rm seed
```

The seed service mounts `./data` from the host, waits for both databases to be healthy, loads all CSV files, then exits. Nothing is copied into the image.

### 4. Verify

```bash
# Check PostgreSQL
docker compose exec postgres \
  psql -U ${DB_USER} -d ${DB_NAME} -c "SELECT count(*) FROM prescription_data;"

# Check MongoDB
docker compose exec mongo \
  mongosh --quiet --eval "db.getSiblingDB('${MONGO_DB}').prescription_data.countDocuments()"
```

### Seeding via CI/CD

Add a step to your pipeline that runs after deployment:

```yaml
# GitLab CI example
seed-database:
  stage: deploy
  script:
    - docker compose up -d
    - docker compose run --rm seed
  only:
    - main
  when: manual   # run once, not on every push
```

### Re-seeding or tearing down

```bash
# Re-seed (rebuild to pick up code changes)
docker compose run --rm --build seed

# View seed logs
docker compose logs seed

# Tear everything down
docker compose down      # keeps data
docker compose down -v   # removes data too
```

## Key design decisions

- **Chunked reading** — each CSV is streamed in configurable chunks so files larger than available RAM can be processed.
- **Context-manager support** — loaders implement `__enter__`/`__exit__` for automatic resource cleanup.
- **Composable transforms** — each transform is a pure function `DataFrame → DataFrame`, easy to test and reorder.
- **Environment-based config** — secrets stay out of source code; different environments just set different env vars.
- **Directory-based loading** — the pipeline scans `DATA_DIR` for all `.csv` files and loads each into its own table/collection, so adding a new file requires no code changes.
- **Single compose file** — `docker-compose.yml` covers both local dev (run databases only) and deployment (add the `seed` service via `docker compose run`).

## Dataset

### Source

The data originates from two Swedish public health authorities:

- **Socialstyrelsen** (National Board of Health and Welfare) — prescription statistics 2006–2024, available via the open statistics API at [socialstyrelsen.se/statistik-och-data/statistik/for-utvecklare](https://www.socialstyrelsen.se/statistik-och-data/statistik/for-utvecklare/) (CSV Statistikdatabasen – Läkemedel 2006–2024).
- **Läkemedelsverket** (Medical Products Agency) — narcotic classification per ATC code, extracted from the Nationellt produktregister för läkemedel (NPL).

### Preprocessing and filtering

The raw Socialstyrelsen export covers all dispensed prescriptions. The scripts in `scripts/` reduce this to narcotic drugs only:

1. `narcotics_extractor.py` — parses NPL XML product files and builds a mapping of ATC codes to narcotic class (II–V).
2. `preprocessing.py` — filters the raw prescription data to only rows whose ATC code appears in that mapping, then joins in drug names and generates the four lookup tables.

**`prescription_data.csv` therefore contains only narcotic-classified prescriptions** — not all dispensed drugs. This is intentional: the dataset is scoped to controlled substances for focused analysis.

### Entities

| File | Rows | Description |
|---|---|---|
| `prescription_data.csv` | ~1.88 M | Main fact table — one row per (year, region, drug, gender, age group) combination |
| `drugs.csv` | 79 | Lookup — narcotic-classified drugs with ATC code and Swedish name |
| `regions.csv` | 22 | Lookup — Swedish regions (counties + national total "Riket") |
| `genders.csv` | 2 | Lookup — gender categories (Män / Kvinnor) |
| `age_groups.csv` | 19 | Lookup — five-year age bands (0–4, 5–9, … 90+) |

### Key fields — `prescription_data.csv`

| Field | Type | Description |
|---|---|---|
| `year` | int | Calendar year (2006–2024) |
| `region` | int | Region ID (FK → `regions.id`) |
| `atc` | string | 7-character ATC code (FK → `drugs.atc`) |
| `gender` | int | Gender ID (FK → `genders.id`) |
| `age_group` | int | Age group ID (FK → `age_groups.id`) |
| `num_prescriptions` | int | Number of dispensed prescriptions |
| `num_patients` | int | Number of unique patients |
| `per_1000` | float | Dispensations per 1,000 inhabitants |
| `narcotic_class` | string | Narcotic schedule (II, III, IV, or V) |
