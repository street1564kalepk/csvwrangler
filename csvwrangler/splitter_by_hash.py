"""CSVHashSplitter – split rows into N buckets by hashing a key column."""
from __future__ import annotations

import hashlib
from typing import Dict, List


class CSVHashSplitter:
    """Split rows deterministically into *n_buckets* groups by hashing *column*."""

    def __init__(self, source, column: str, n_buckets: int = 2) -> None:
        if n_buckets < 1:
            raise ValueError("n_buckets must be >= 1")
        self._source = source
        self._column = column
        self._n_buckets = n_buckets
        self._built: bool = False
        self._groups: Dict[int, List[dict]] = {i: [] for i in range(n_buckets)}

    # ------------------------------------------------------------------
    @property
    def headers(self) -> List[str]:
        return list(self._source.headers)

    def _ensure_built(self) -> None:
        if self._built:
            return
        col = self._column
        if col not in self._source.headers:
            raise KeyError(f"Column '{col}' not found in headers")
        for row in self._source.rows():
            raw = str(row.get(col, ""))
            digest = int(hashlib.md5(raw.encode()).hexdigest(), 16)
            bucket = digest % self._n_buckets
            self._groups[bucket].append(row)
        self._built = True

    # ------------------------------------------------------------------
    @property
    def group_keys(self) -> List[int]:
        self._ensure_built()
        return sorted(self._groups.keys())

    def bucket_rows(self, bucket: int) -> List[dict]:
        self._ensure_built()
        if bucket not in self._groups:
            raise KeyError(f"Bucket {bucket} does not exist")
        return list(self._groups[bucket])

    @property
    def bucket_count(self) -> int:
        return self._n_buckets

    def row_count(self, bucket: int) -> int:
        return len(self.bucket_rows(bucket))
