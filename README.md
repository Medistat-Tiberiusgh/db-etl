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

# 3. Start the database
docker compose up -d

# 4. Seed the database
docker compose run --rm seed
```

## Production / VPS deployment

Everything runs inside Docker — no Python or uv needed on the host.
The CSV files are **not** baked into the Docker image. Instead, the `data/` directory on the host is mounted into the container as a read-only volume.

## Key design decisions

- **Chunked reading** — each CSV is streamed in configurable chunks so files larger than available RAM can be processed.
- **Context-manager support** — loaders implement `__enter__`/`__exit__` for automatic resource cleanup.
- **Composable transforms** — each transform is a pure function `DataFrame → DataFrame`, easy to test and extend. The dataset is pre-processed and clean, so only column normalisation is applied at load time.
- **Environment-based config** — secrets stay out of source code; different environments just set different env vars.
- **Directory-based loading** — the pipeline scans `DATA_DIR` for all `.csv` files and loads each into its own table, so adding a new file requires no code changes.
- **Single compose file** — `docker-compose.yml` covers both local dev and deployment using the same `seed` service via `docker compose run`.

## Dataset

### Source

The data originates from two Swedish public health authorities:

- **Socialstyrelsen** (National Board of Health and Welfare) — prescription statistics 2006–2024 for all human drugs, available via the open statistics API at [socialstyrelsen.se/statistik-och-data/statistik/for-utvecklare](https://www.socialstyrelsen.se/statistik-och-data/statistik/for-utvecklare/) (CSV Statistikdatabasen – Läkemedel 2006–2024). Approximately 46 million rows, partitioned by year, region, gender, and age group.
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
| `age_groups.csv`        | 19    | Read-only resource          | Five-year age bands (0–4, 5–9, … 90+) plus total                                  |

### Key fields — `prescription_data.csv`

| Field               | Type   | Description                             |
| ------------------- | ------ | --------------------------------------- |
| `year`              | int    | Calendar year (2006–2024)               |
| `region`            | int    | Region ID (FK → `regions.id`)           |
| `atc`               | string | 7-character ATC code (FK → `drugs.atc`) |
| `gender`            | int    | Gender ID (FK → `genders.id`)           |
| `age_group`         | int    | Age group ID (FK → `age_groups.id`)     |
| `num_prescriptions` | int    | Number of dispensed prescriptions       |
| `num_patients`      | int    | Number of unique patients               |
| `per_1000`          | float  | Dispensations per 1,000 inhabitants     |
