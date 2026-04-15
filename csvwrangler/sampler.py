"""CSVSampler: randomly sample N rows or a fraction of rows from a CSV source."""

import random
from typing import Iterator, Optional


class CSVSampler:
    """Randomly sample rows from a CSV source.

    Supports two modes:
      - Fixed count: sample exactly *n* rows (reservoir sampling).
      - Fraction: sample each row independently with probability *frac*.

    Args:
        source: Any object exposing ``.headers`` and ``.rows()``.
        n: Number of rows to sample (mutually exclusive with *frac*).
        frac: Fraction of rows to sample, in the range (0, 1].
        seed: Optional random seed for reproducibility.
    """

    def __init__(self, source, *, n: Optional[int] = None, frac: Optional[float] = None, seed=None):
        if n is None and frac is None:
            raise ValueError("Specify either 'n' or 'frac'.")
        if n is not None and frac is not None:
            raise ValueError("Specify only one of 'n' or 'frac', not both.")
        if n is not None and n < 0:
            raise ValueError("'n' must be a non-negative integer.")
        if frac is not None and not (0 < frac <= 1):
            raise ValueError("'frac' must be in the range (0, 1].")

        self._source = source
        self._n = n
        self._frac = frac
        self._rng = random.Random(seed)

    @property
    def headers(self):
        return self._source.headers

    def rows(self) -> Iterator[dict]:
        if self._n is not None:
            yield from self._reservoir_sample(self._n)
        else:
            yield from self._fraction_sample(self._frac)

    def _reservoir_sample(self, n: int) -> list:
        """Reservoir sampling — O(N) time, O(n) space."""
        reservoir = []
        for i, row in enumerate(self._source.rows()):
            if i < n:
                reservoir.append(row)
            else:
                j = self._rng.randint(0, i)
                if j < n:
                    reservoir[j] = row
        # Shuffle so output order is random, not biased toward early rows.
        self._rng.shuffle(reservoir)
        yield from reservoir

    def _fraction_sample(self, frac: float) -> Iterator[dict]:
        for row in self._source.rows():
            if self._rng.random() < frac:
                yield row

    def row_count(self) -> int:
        """Materialise all sampled rows and return the count."""
        return sum(1 for _ in self.rows())

    def __repr__(self) -> str:  # pragma: no cover
        mode = f"n={self._n}" if self._n is not None else f"frac={self._frac}"
        return f"CSVSampler({mode})"
