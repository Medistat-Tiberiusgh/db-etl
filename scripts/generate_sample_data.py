"""
Generate synthetic prescription_data rows for CI/testing.

Reads valid FK values from sample/ lookup tables and produces fully
synthetic rows — no dependency on the real prescription_data.csv.

Usage as a script (smoke-test / manual inspection):
    python scripts/generate_sample_data.py
    python scripts/generate_sample_data.py --rows 1000 --seed 7

Usage as a module (from pytest fixtures):
    from scripts.generate_sample_data import generate_rows
    rows = generate_rows(rows=200)
"""

import argparse
import csv
import random
from pathlib import Path

SAMPLE_DIR = Path(__file__).parent.parent / "sample"

YEARS = list(range(2006, 2024))
DEFAULT_ROWS = 500
DEFAULT_SEED = 42
ZERO_ROW_PROBABILITY = 0.25  # ~25 % of rows have zero prescriptions, matching real-data distribution


def _load_column(path: Path, column: str) -> list[str]:
    with open(path, newline="", encoding="utf-8") as f:
        return [row[column] for row in csv.DictReader(f)]


def _load_drugs(path: Path) -> list[dict]:
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def generate_rows(rows: int = DEFAULT_ROWS, seed: int = DEFAULT_SEED) -> list[dict]:
    """Return a list of synthetic prescription dicts with valid FK values."""
    random.seed(seed)

    drugs = _load_drugs(SAMPLE_DIR / "drugs.csv")
    regions = _load_column(SAMPLE_DIR / "regions.csv", "id")
    genders = _load_column(SAMPLE_DIR / "genders.csv", "id")
    age_groups = _load_column(SAMPLE_DIR / "age_groups.csv", "id")

    result = []
    for _ in range(rows):
        drug = random.choice(drugs)
        is_zero = random.random() < ZERO_ROW_PROBABILITY
        num_prescriptions = 0 if is_zero else random.randint(1, 500)
        num_patients = 0 if is_zero else random.randint(1, num_prescriptions)
        per_1000 = 0.0 if is_zero else round(random.uniform(0.1, 50.0), 1)

        result.append({
            "year": random.choice(YEARS),
            "region": random.choice(regions),
            "atc": drug["atc"],
            "gender": random.choice(genders),
            "age_group": random.choice(age_groups),
            "num_prescriptions": num_prescriptions,
            "num_patients": num_patients,
            "per_1000": per_1000,
            "narcotic_class": drug["narcotic_class"],
        })

    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate synthetic prescription data for testing")
    parser.add_argument("--rows", type=int, default=DEFAULT_ROWS)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    args = parser.parse_args()

    data = generate_rows(rows=args.rows, seed=args.seed)

    fieldnames = list(data[0].keys())
    print(",".join(fieldnames))
    for row in data:
        print(",".join(str(row[f]) for f in fieldnames))
