FROM python:3.13-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

COPY src/ src/
COPY main.py api_ingest.py scheduler.py ./

# The same image serves three roles, chosen by the command:
#   (default) main.py          -> bulk CSV seed
#   -m scheduler               -> always-on weekly ingest (compose 'scheduler')
#   -m api_ingest              -> one-shot manual ingest
ENTRYPOINT ["uv", "run", "python"]
CMD ["main.py"]
