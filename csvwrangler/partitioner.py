"""CSVPartitioner – split rows into N roughly equal buckets."""
from __future__ import annotations

import math
from typing import List


class CSVPartitioner:
    """Distribute rows across *n_parts* partitions in round-robin order.

    Each partition is a list of row dicts that can be iterated independently.
    """

    def __init__(self, source, n_parts: int) -> None:
        if n_parts < 1:
            raise ValueError("n_parts must be >= 1")
        self._source = source
        self._n = n_parts
        self._buckets: List[List[dict]] | None = None

    # ------------------------------------------------------------------
    @property
    def headers(self) -> List[str]:
        return list(self._source.headers)

    # ------------------------------------------------------------------
    def _ensure_built(self) -> None:
        if self._buckets is not None:
            return
        self._buckets = [[] for _ in range(self._n)]
        for idx, row in enumerate(self._source.rows()):
            self._buckets[idx % self._n].append(row)

    # ------------------------------------------------------------------
    @property
    def partition_count(self) -> int:
        return self._n

    def partition(self, index: int) -> List[dict]:
        """Return all rows assigned to *index* (0-based)."""
        if index < 0 or index >= self._n:
            raise IndexError(f"index {index} out of range for {self._n} partitions")
        self._ensure_built()
        return list(self._buckets[index])  # type: ignore[index]

    def sizes(self) -> List[int]:
        """Return the number of rows in each partition."""
        self._ensure_built()
        return [len(b) for b in self._buckets]  # type: ignore[union-attr]

    def rows(self, index: int):
        """Iterate over rows in partition *index*."""
        yield from self.partition(index)

    def row_count(self) -> int:
        """Total rows across all partitions."""
        self._ensure_built()
        return sum(len(b) for b in self._buckets)  # type: ignore[union-attr]

    def __repr__(self) -> str:  # pragma: no cover
        return f"CSVPartitioner(n_parts={self._n})"
