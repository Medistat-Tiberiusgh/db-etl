"""
Data-cleaning transformations applied to each chunk.

Each public function takes a DataFrame and returns a new DataFrame,
making them easy to compose, test, and extend independently.
"""

import pandas as pd


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Lowercase column names and strip surrounding whitespace."""
    df = df.copy()
    df.columns = [col.strip().lower() for col in df.columns]
    return df


def fill_missing(df: pd.DataFrame, default: str = "Unknown") -> pd.DataFrame:
    """Replace NaN / None values with *default*."""
    return df.fillna(default)


def apply_transforms(df: pd.DataFrame) -> pd.DataFrame:
    """Run the full transformation pipeline on a single chunk."""
    df = normalize_columns(df)
    df = fill_missing(df)
    return df
