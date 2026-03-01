"""
Generate a representative sample of prescription_data.csv for CI/testing.

Strategy:
  - Filter to ATCs present in sample/drugs.csv (narcotics only) so that all
    rows are consistent with the lookup tables already in sample/.
  - Take a stratified random sample: ROWS_PER_GROUP rows per (year, region)
    combination, covering the full breadth of the dataset.
"""

import csv
import random
from pathlib import Path

ROOT = Path(__file__).parent.parent
DATA_FILE = ROOT / "data" / "prescription_data.csv"
SAMPLE_DIR = ROOT / "sample"
DRUGS_FILE = SAMPLE_DIR / "drugs.csv"
OUTPUT_FILE = SAMPLE_DIR / "prescription_data.csv"

ROWS_PER_GROUP = 20
SEED = 42


def load_narcotic_atcs(path: Path) -> set[str]:
    with open(path, newline="", encoding="utf-8") as f:
        return {row["atc"] for row in csv.DictReader(f)}


def main() -> None:
    random.seed(SEED)

    narcotic_atcs = load_narcotic_atcs(DRUGS_FILE)
    print(f"Loaded {len(narcotic_atcs)} narcotic ATCs from {DRUGS_FILE.name}")

    groups: dict[tuple[str, str], list[dict]] = {}
    with open(DATA_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        for row in reader:
            if row["atc"] not in narcotic_atcs:
                continue
            key = (row["year"], row["region"])
            groups.setdefault(key, []).append(row)

    print(f"Found {len(groups)} (year, region) groups after filtering")

    sample: list[dict] = []
    for rows in groups.values():
        sample.extend(random.sample(rows, min(ROWS_PER_GROUP, len(rows))))

    sample.sort(key=lambda r: (r["year"], r["region"], r["atc"]))

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(sample)

    print(f"Written {len(sample)} rows → {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
