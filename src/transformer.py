"""
Data cleaning applied to each chunk before loading.

The source CSVs are already filtered, renamed, and lowercased by
scripts/preprocessing.py. The only thing left to fix at load time is the
Swedish decimal format in the per_1000 column ("2,68" → 2.68), which
PostgreSQL COPY cannot parse into a NUMERIC column.
"""

import pandas as pd


def apply_transforms(df: pd.DataFrame) -> pd.DataFrame:
    """Convert the Swedish-formatted per_1000 rate to a real number, if present."""
    if "per_1000" not in df.columns:
        return df

    df = df.copy()
    df["per_1000"] = pd.to_numeric(df["per_1000"].str.replace(",", ".", regex=False))
    return df
