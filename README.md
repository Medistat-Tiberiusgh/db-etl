# From CSV to Database — ETL Pipeline

A teaching example that loads a large CSV file into **PostgreSQL** and/or **MongoDB**, structured around clean code and SOLID principles.

## Project structure

```
.
├── main.py                   # CLI entry point
├── Dockerfile                # Container image for the ETL app
├── docker-compose.yml        # Local dev — databases only
├── docker-compose.prod.yml   # Production — databases + seed service
├── seed/
│   └── customers.csv         # Small sample (committed to repo)
├── data/                     # ⬅ Place the large CSV here (gitignored)
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
│   └── generate_csv.py       # Optional: generate synthetic test data
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

## Data file

A small sample dataset is included in `seed/customers.csv` so the project works out of the box.

**For the full exercise**, download the large dataset from Moodle and place it in the `data/` directory:

```
data/customers.csv    # ~50 MB, 500K+ rows
```

Then update `CSV_PATH` in your `.env` file:

```
CSV_PATH=data/customers.csv
```

The `data/` directory is gitignored — large files should never be committed to the repository.

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

By default the pipeline loads `seed/customers.csv`. To use the large dataset from Moodle:

```bash
# Either edit .env (CSV_PATH=data/customers.csv) or pass it inline:
CSV_PATH=data/customers.csv uv run python main.py
```

## Production / VPS deployment

Everything runs inside Docker — no Python or uv needed on the host.
The large CSV file is **not** baked into the Docker image. Instead, the `data/` directory on the host is mounted into the container as a read-only volume — the container reads the file directly from disk without copying it.

### 1. Upload the data file

The large CSV needs to end up in the `data/` directory on the server. There are several ways to get it there.

**Option A — Copy from your local machine via SSH:**

```bash
# Make sure the data/ directory exists on the server
ssh user@your-server "mkdir -p ~/from-csv-to-database/data"

# Copy the file (replace user and your-server with your actual credentials)
scp /path/to/customers.csv user@your-server:~/from-csv-to-database/data/customers.csv
```

If you're using an SSH key (`.pem` file) instead of a password:

```bash
scp -i ~/.ssh/your-key.pem /path/to/customers.csv user@your-server:~/from-csv-to-database/data/
```

**Option B — Download directly on the server from a remote URL:**

If the file is hosted somewhere (e.g. AWS S3, a shared link, or your university's file server), you can download it straight to the VPS without going through your local machine:

```bash
ssh user@your-server
cd ~/from-csv-to-database
mkdir -p data

# From a public URL
curl -o data/customers.csv https://example.com/customers.csv

# From AWS S3 (requires aws cli configured on the server)
aws s3 cp s3://your-bucket/customers.csv data/customers.csv
```

This is faster for large files since data goes directly to the server instead of routing through your laptop.

### 2. Start the databases

```bash
docker compose -f docker-compose.prod.yml up -d
```

This starts **PostgreSQL** and **MongoDB** with health checks. They stay running in the background.

### 3. Seed the databases (run once)

```bash
docker compose -f docker-compose.prod.yml run --rm seed
```

The seed service mounts `./data` from the host, waits for both databases to be healthy, loads the CSV, then exits. Nothing is copied into the image.

### 4. Verify

```bash
# Check PostgreSQL
docker compose -f docker-compose.prod.yml exec postgres \
  psql -U etl_user -d etl_demo -c "SELECT count(*) FROM customers;"

# Check MongoDB
docker compose -f docker-compose.prod.yml exec mongo \
  mongosh --quiet --eval "db.getSiblingDB('etl_demo').customers.countDocuments()"
```

### Seeding via CI/CD

Add a step to your pipeline that runs after deployment:

```yaml
# GitLab CI example
seed-database:
  stage: deploy
  script:
    - docker compose -f docker-compose.prod.yml up -d
    - docker compose -f docker-compose.prod.yml run --rm seed
  only:
    - main
  when: manual   # run once, not on every push
```

### Re-seeding or tearing down

```bash
# Re-seed (rebuild to pick up code changes)
docker compose -f docker-compose.prod.yml run --rm --build seed

# View seed logs
docker compose -f docker-compose.prod.yml logs seed

# Tear everything down
docker compose -f docker-compose.prod.yml down      # keeps data
docker compose -f docker-compose.prod.yml down -v    # removes data too
```

## Key design decisions

- **Chunked reading** — the CSV is streamed in configurable chunks so files larger than available RAM can be processed.
- **Context-manager support** — loaders implement `__enter__`/`__exit__` for automatic resource cleanup.
- **Composable transforms** — each transform is a pure function `DataFrame → DataFrame`, easy to test and reorder.
- **Environment-based config** — secrets stay out of source code; different environments just set different env vars.
- **Two compose files** — `docker-compose.yml` for local dev (databases only, fast iteration), `docker-compose.prod.yml` for deployment (everything containerized).
- **Seed data in the repo** — `seed/customers.csv` is a small committed sample so the project works immediately. The large dataset is distributed separately and kept out of version control.
