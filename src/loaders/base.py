"""
Abstract loader interface (Dependency-Inversion Principle).

The pipeline depends on this abstraction rather than concrete
database clients, so new targets can be added without touching
existing code (Open/Closed Principle).
"""

from abc import ABC, abstractmethod

import pandas as pd


class Loader(ABC):
    """Contract every database loader must fulfil."""

    @abstractmethod
    def load(self, df: pd.DataFrame) -> None:
        """Persist a single chunk of data."""

    @abstractmethod
    def close(self) -> None:
        """Release any held resources (connections, cursors, etc.)."""

    # Allow use as a context manager for automatic cleanup.
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
