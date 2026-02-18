"""
Generate a large synthetic CSV for testing the ETL pipeline.

Usage:
    python scripts/generate_csv.py                  # 500 000 rows (default)
    python scripts/generate_csv.py --rows 1000000   # 1 million rows
"""

import argparse
import csv
import random
import string
import sys
from datetime import datetime, timedelta
from pathlib import Path

DOMAINS = ["gmail.com", "outlook.com", "yahoo.com", "proton.me", "icloud.com"]
PLANS = ["free", "starter", "pro", "enterprise"]
COUNTRIES = [
    "Sweden", "Norway", "Denmark", "Finland", "Germany",
    "France", "UK", "USA", "Canada", "Japan",
]
CITIES = {
    "Sweden": ["Stockholm", "Gothenburg", "Malmö", "Uppsala"],
    "Norway": ["Oslo", "Bergen", "Trondheim"],
    "Denmark": ["Copenhagen", "Aarhus", "Odense"],
    "Finland": ["Helsinki", "Espoo", "Tampere"],
    "Germany": ["Berlin", "Munich", "Hamburg", "Frankfurt"],
    "France": ["Paris", "Lyon", "Marseille", "Toulouse"],
    "UK": ["London", "Manchester", "Birmingham", "Edinburgh"],
    "USA": ["New York", "San Francisco", "Chicago", "Austin"],
    "Canada": ["Toronto", "Vancouver", "Montreal"],
    "Japan": ["Tokyo", "Osaka", "Kyoto"],
}
FIRST_NAMES = [
    "Emma", "Liam", "Olivia", "Noah", "Ava", "Elias", "Sophia", "William",
    "Isabella", "James", "Mia", "Oliver", "Charlotte", "Lucas", "Amelia",
    "Erik", "Astrid", "Lars", "Freya", "Magnus", "Ingrid", "Sven", "Linnea",
]
LAST_NAMES = [
    "Andersson", "Johansson", "Karlsson", "Nilsson", "Eriksson", "Larsson",
    "Olsson", "Persson", "Smith", "Johnson", "Williams", "Brown", "Jones",
    "Garcia", "Miller", "Davis", "Müller", "Schmidt", "Schneider", "Fischer",
    "Tanaka", "Suzuki", "Watanabe", "Takahashi",
]

OUTPUT_PATH = Path("data/customers.csv")
FIELDNAMES = [
    "id", "first_name", "last_name", "email", "phone",
    "country", "city", "signup_date", "plan", "monthly_spend",
]
START_DATE = datetime(2020, 1, 1)
DATE_RANGE_DAYS = (datetime(2025, 12, 31) - START_DATE).days


def random_phone() -> str:
    return f"+{random.randint(1,99)}-{random.randint(100,999)}-{random.randint(1000000,9999999)}"


def random_date() -> str:
    return (START_DATE + timedelta(days=random.randint(0, DATE_RANGE_DAYS))).strftime("%Y-%m-%d")


def make_row(row_id: int) -> dict:
    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)
    country = random.choice(COUNTRIES)
    plan = random.choice(PLANS)

    # ~5 % chance of missing values to test the transform step
    email = (
        f"{first.lower()}.{last.lower()}{random.randint(1,999)}@{random.choice(DOMAINS)}"
        if random.random() > 0.05
        else ""
    )

    return {
        "id": row_id,
        "first_name": first,
        "last_name": last,
        "email": email,
        "phone": random_phone() if random.random() > 0.05 else "",
        "country": country,
        "city": random.choice(CITIES[country]),
        "signup_date": random_date(),
        "plan": plan,
        "monthly_spend": round(random.uniform(0, 500), 2) if plan != "free" else 0.00,
    }


def generate(rows: int) -> None:
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    with OUTPUT_PATH.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()

        for i in range(1, rows + 1):
            writer.writerow(make_row(i))
            if i % 100_000 == 0:
                print(f"  {i:>10,} rows written …")

    size_mb = OUTPUT_PATH.stat().st_size / (1024 * 1024)
    print(f"Done — {rows:,} rows written to {OUTPUT_PATH} ({size_mb:.1f} MB)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate a synthetic customer CSV")
    parser.add_argument("--rows", type=int, default=500_000, help="Number of rows (default: 500 000)")
    args = parser.parse_args()
    generate(args.rows)
