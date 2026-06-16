"""
Extractor for the Socialstyrelsen statistics API (läkemedel dataset).

Used for the yearly incremental ingest, as a counterpart to the bulk CSV
extractor. It produces the same row shape as the CSV seed so the rest of the
pipeline (transform + load) is unchanged.

API shape (discovered from the live service):
  GET /resultat/atc/{atc,…}/matt/{ONE}/ar/{year}
    -> {"data": [{atcId, regionId, alderId, konId, mattId, ar, varde}, …],
        "nasta_sida": <url|null>, "sida", "per_sida", "sidor"}

Key facts that shape this code:
  - `matt` (measure) accepts only ONE id per request, so the three metrics are
    fetched separately and pivoted back together per cell.
  - Omitting region/kon/alder returns them broken down; omitting atc collapses
    to a "TOTALT" aggregate, so ATC codes must be listed explicitly.
  - The API is sparse: cells with no data are simply absent.
  - `varde` is a string; per_1000 is Swedish-formatted ("0,03").
"""

import logging
import time
from collections.abc import Iterator

import pandas as pd
import requests

logger = logging.getLogger(__name__)

BASE_URL = "https://sdb.socialstyrelsen.se/api/v1/sv/lakemedel"

# API measure id -> our column. One measure per request (the API rejects lists).
MEASURES = {
    3: "num_prescriptions",
    1: "num_patients",
    4: "per_1000",
}

COLUMNS = [
    "year", "region", "atc", "gender", "age_group",
    "num_prescriptions", "num_patients", "per_1000",
]


class SocialstyrelsenApi:
    """Reads the läkemedel dataset one year at a time, in ATC batches."""

    def __init__(
        self,
        base_url: str = BASE_URL,
        atc_batch_size: int = 50,
        max_retries: int = 4,
        timeout: int = 60,
    ) -> None:
        self._base_url = base_url
        self._atc_batch_size = atc_batch_size
        self._max_retries = max_retries
        self._timeout = timeout
        self._session = requests.Session()

    def available_years(self) -> list[int]:
        """All years the dataset offers, ascending."""
        rows = self._request(f"{self._base_url}/ar")
        return sorted(int(row["id"]) for row in rows)

    def latest_year(self) -> int:
        return max(self.available_years())

    def drug_names(self) -> dict[str, str]:
        """
        {atc: name} for the human-drug substance codes, matching the bulk seed's
        filter: 7-character ATC, excluding the "TOTALT" grand total and Q-prefix
        veterinary codes. Used to upsert new drugs before loading a year.
        """
        rows = self._request(f"{self._base_url}/atc")
        return {
            row["id"]: row["text"]
            for row in rows
            if _is_substance_code(row["id"])
        }

    def leaf_atc_codes(self) -> list[str]:
        """The substance codes to query, i.e. the keys of drug_names()."""
        return list(self.drug_names())

    def fetch_year(self, year: int, atc_codes: list[str]) -> Iterator[pd.DataFrame]:
        """Yield one DataFrame chunk per ATC batch, shaped like prescription_data."""
        for batch in _batched(atc_codes, self._atc_batch_size):
            yield self._fetch_batch(year, batch)

    def _fetch_batch(self, year: int, atc_codes: list[str]) -> pd.DataFrame:
        cells: dict[tuple, dict] = {}
        for measure_id, column in MEASURES.items():
            for record in self._results(year, atc_codes, measure_id):
                key = (
                    record["atcId"], record["regionId"],
                    record["konId"], record["alderId"],
                )
                row = cells.setdefault(key, _empty_row(year, key))
                row[column] = record["varde"]
        return pd.DataFrame(cells.values(), columns=COLUMNS)

    def _results(self, year: int, atc_codes: list[str], measure_id: int) -> Iterator[dict]:
        atc_segment = ",".join(atc_codes)
        url = (
            f"{self._base_url}/resultat"
            f"/atc/{atc_segment}/matt/{measure_id}/ar/{year}"
        )
        while url:
            page = self._request(url)
            yield from page["data"]
            url = page.get("nasta_sida")

    def _request(self, url: str) -> dict | list:
        last_error: Exception | None = None
        for attempt in range(1, self._max_retries + 1):
            try:
                response = self._session.get(url, timeout=self._timeout)
                response.raise_for_status()
                return response.json()
            except (requests.RequestException, ValueError) as error:
                last_error = error
                backoff = 2 ** attempt
                logger.warning(
                    "API request failed (attempt %d/%d): %s — retrying in %ds",
                    attempt, self._max_retries, error, backoff,
                )
                time.sleep(backoff)
        raise RuntimeError(f"API request failed after {self._max_retries} attempts: {url}") from last_error


def _empty_row(year: int, key: tuple) -> dict:
    """A cell defaulted to zero; absent measures stay zero and get dropped later."""
    atc, region, gender, age_group = key
    return {
        "year": year, "region": region, "atc": atc,
        "gender": gender, "age_group": age_group,
        "num_prescriptions": "0", "num_patients": "0", "per_1000": "0",
    }


def _is_substance_code(atc: str) -> bool:
    """A human-drug leaf: 7-character ATC that isn't a Q-prefix veterinary code."""
    return len(atc) == 7 and not atc.startswith("Q")


def _batched(items: list, size: int) -> Iterator[list]:
    for start in range(0, len(items), size):
        yield items[start:start + size]
