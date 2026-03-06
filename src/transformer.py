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


def coerce_numeric(df: pd.DataFrame) -> pd.DataFrame:
    """Fix locale-formatted floats (e.g. '0,01' → 0.01) that pandas reads as strings."""
    df = df.copy()
    for col in df.select_dtypes(include="object").columns:
        converted = df[col].str.replace(",", ".", regex=False)
        try:
            df[col] = pd.to_numeric(converted)
        except (ValueError, TypeError):
            pass
    return df


def apply_transforms(df: pd.DataFrame) -> pd.DataFrame:
    """Run the full transformation pipeline on a single chunk."""
    df = normalize_columns(df)
    df = coerce_numeric(df)
    return df
