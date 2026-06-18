"""
Extractor for the Socialstyrelsen statistics API (läkemedel dataset).

Used for the yearly incremental ingest, as a counterpart to the bulk CSV
extractor. It produces the same row shape as the CSV seed (one row per cell),
except the all-ages age_group = 99 total, which the API has no concept of and
the ingest computes from the bands afterwards (see api_ingest.add_age_totals).

API shape (discovered from the live service):
  GET /resultat/atc/{atc}/matt/{ONE}/ar/{year}
    -> {"data": [{atcId, regionId, alderId, konId, mattId, ar, varde}, …],
        "nasta_sida": <url|null>, "sida", "per_sida", "sidor"}

Key facts that shape this code:
  - `matt` (measure) accepts only ONE id per request, so the three metrics are
    fetched separately and pivoted back together per cell.
  - Omitting region/kon/alder returns them broken down; omitting atc collapses
    to a "TOTALT" aggregate, so ATC codes must be listed explicitly.
  - Request ONE ATC code at a time. Asking for many at once silently truncates
    the result: `nasta_sida` pagination stops after ~2 pages, returning only
    ~58% of the rows. A single ATC is at most 22×3×18 = 1188 rows (one page),
    so it always comes back complete.
  - There is no age "Totalt": the age dimension only has bands 1–18, so the
    all-ages roll-up (age_group = 99) is computed downstream in the ingest.
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

# Identify ourselves on every request: a contact address is good manners for a
# public API (lets the maintainers reach us instead of silently blocking an
# anonymous client). The Client-Hint headers are real headers; their values are
# a joke.
DEFAULT_HEADERS = {
    "User-Agent": "tiberius.gherac@gmail.com",
    "Sec-CH-UA": '"Netscape Navigator";v="4", "Mosaic";v="1"',
    "Sec-CH-UA-Platform": '"Windows 3.1"',
}

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
        max_retries: int = 4,
        timeout: int = 60,
    ) -> None:
        self._base_url = base_url
        self._max_retries = max_retries
        self._timeout = timeout
        self._session = requests.Session()
        self._session.headers.update(DEFAULT_HEADERS)

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
        """Yield one DataFrame per ATC code, shaped like prescription_data.

        Logs progress periodically: one ATC per request means ~2000 requests per
        year, so without it the ingest looks frozen for several minutes.
        """
        total = len(atc_codes)
        for index, atc in enumerate(atc_codes, start=1):
            chunk = self._fetch_drug(year, atc)
            if index % 200 == 0 or index == total:
                logger.info("Year %d: fetched %d/%d drugs", year, index, total)
            yield chunk

    def _fetch_drug(self, year: int, atc: str) -> pd.DataFrame:
        cells: dict[tuple, dict] = {}
        for measure_id, column in MEASURES.items():
            for record in self._results(year, atc, measure_id):
                key = (
                    record["atcId"], record["regionId"],
                    record["konId"], record["alderId"],
                )
                row = cells.setdefault(key, _empty_row(year, key))
                row[column] = record["varde"]
        return pd.DataFrame(cells.values(), columns=COLUMNS)

    def _results(self, year: int, atc: str, measure_id: int) -> Iterator[dict]:
        """Yield the data rows for one drug/measure/year, following pagination.

        A 404 means this (valid) drug has no data for the measure/year — the
        API's way of saying "empty" — so we stop rather than treat it as a
        failure. We only ever build URLs from validated inputs (year from
        available_years, a known measure id, an ATC code from the API's own
        catalog), so a 404 here cannot be a malformed request. Network problems,
        by contrast, surface as connection/timeout errors (retried, then raised
        loudly) — never as a 404 — so they can't masquerade as "no data".
        """
        url = (
            f"{self._base_url}/resultat"
            f"/atc/{atc}/matt/{measure_id}/ar/{year}"
        )
        while url:
            page = self._request_optional(url)
            if page is None:
                logger.debug("No data for atc=%s measure=%d year=%d", atc, measure_id, year)
                return
            yield from page["data"]
            url = page.get("nasta_sida")

    def _request_optional(self, url: str) -> dict | list | None:
        """A GET whose resource may legitimately not exist; returns None on 404."""
        try:
            return self._request(url)
        except requests.HTTPError as error:
            if _is_not_found(error):
                return None
            raise

    def _request(self, url: str) -> dict | list:
        """GET parsed JSON, retrying transient failures.

        A 404 is definitive (the resource genuinely doesn't exist), not
        transient, so it is raised immediately rather than retried.
        """
        last_error: Exception | None = None
        for attempt in range(1, self._max_retries + 1):
            try:
                response = self._session.get(url, timeout=self._timeout)
                response.raise_for_status()
                return response.json()
            except requests.HTTPError as error:
                if _is_not_found(error):
                    raise
                last_error = error
            except (requests.RequestException, ValueError) as error:
                last_error = error
            self._backoff(attempt, last_error)
        raise RuntimeError(f"API request failed after {self._max_retries} attempts: {url}") from last_error

    def _backoff(self, attempt: int, error: Exception | None) -> None:
        """Log a failed attempt and wait, with the delay growing per attempt."""
        delay = 2 ** attempt
        logger.warning(
            "API request failed (attempt %d/%d): %s — retrying in %ds",
            attempt, self._max_retries, error, delay,
        )
        time.sleep(delay)


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


def _is_not_found(error: requests.HTTPError) -> bool:
    """True when an HTTP error is a 404 — the API's signal for 'no such data'."""
    return error.response is not None and error.response.status_code == 404
