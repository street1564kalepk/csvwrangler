"""CSVSampleSplitter – splits rows into a sampled subset and a remainder."""
from __future__ import annotations

import random
from typing import Iterable, Iterator


class CSVSampleSplitter:
    """Split a CSV source into a *sampled* group and a *remainder* group.

    Parameters
    ----------
    source:
        Any object exposing ``.headers`` and ``.rows``.
    n:
        Number of rows to include in the sample.  When *n* is greater than or
        equal to the total row count every row ends up in the sample and the
        remainder is empty.
    seed:
        Optional random seed for reproducible splits.
    """

    def __init__(self, source, n: int, *, seed: int | None = None) -> None:
        if n < 0:
            raise ValueError("n must be >= 0")
        self._source = source
        self._n = n
        self._seed = seed
        self._sampled: list[dict] | None = None
        self._remainder: list[dict] | None = None

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    @property
    def headers(self) -> list[str]:
        return list(self._source.headers)

    def _ensure_built(self) -> None:
        if self._sampled is not None:
            return
        all_rows = list(self._source.rows)
        rng = random.Random(self._seed)
        k = min(self._n, len(all_rows))
        sampled_indices = set(rng.sample(range(len(all_rows)), k))
        self._sampled = [row for i, row in enumerate(all_rows) if i in sampled_indices]
        self._remainder = [row for i, row in enumerate(all_rows) if i not in sampled_indices]

    def sampled_rows(self) -> Iterator[dict]:
        self._ensure_built()
        yield from self._sampled  # type: ignore[union-attr]

    def remainder_rows(self) -> Iterator[dict]:
        self._ensure_built()
        yield from self._remainder  # type: ignore[union-attr]

    @property
    def sample_count(self) -> int:
        self._ensure_built()
        return len(self._sampled)  # type: ignore[arg-type]

    @property
    def remainder_count(self) -> int:
        self._ensure_built()
        return len(self._remainder)  # type: ignore[arg-type]

    def __repr__(self) -> str:  # pragma: no cover
        return f"CSVSampleSplitter(n={self._n}, seed={self._seed})"
