"""
Data cleaning applied to each chunk before loading.

Two fixes happen here, for both data sources (the bulk CSV seed and the yearly
API ingest):

  1. The per_1000 rate arrives Swedish-formatted ("2,68"); PostgreSQL COPY
     cannot parse that into a NUMERIC column, so it becomes 2.68.
  2. The bulk CSV pads every dimension combination, so ~70% of rows are empty
     (num_prescriptions = 0). We store data sparsely — a missing row means zero
     prescriptions — so those empty rows are dropped. This keeps the bulk seed
     and the (already sparse) API ingest consistent.
"""

import pandas as pd


def apply_transforms(df: pd.DataFrame) -> pd.DataFrame:
    """Normalise the per_1000 decimal and drop empty prescription rows."""
    df = df.copy()

    if "per_1000" in df.columns:
        df["per_1000"] = parse_swedish_decimal(df["per_1000"])

    if "num_prescriptions" in df.columns:
        prescriptions = pd.to_numeric(df["num_prescriptions"])
        df = df[prescriptions > 0]

    return df


def parse_swedish_decimal(values: pd.Series) -> pd.Series:
    """Turn a Swedish decimal string column ("2,68") into real numbers."""
    if pd.api.types.is_numeric_dtype(values):
        return values
    return pd.to_numeric(values.str.replace(",", ".", regex=False))
