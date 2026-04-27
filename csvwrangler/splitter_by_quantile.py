"""CSVQuantileSplitter – split rows into N quantile buckets based on a numeric column."""
from __future__ import annotations

from typing import Dict, Iterator, List


class CSVQuantileSplitter:
    """Split source rows into *n_quantiles* equal-sized buckets by *column*.

    Bucket labels are ``"Q1"``, ``"Q2"``, … ``"Qn"``.
    Rows whose value cannot be coerced to float are placed in bucket ``"Q_other"``.
    """

    def __init__(self, source, column: str, n_quantiles: int = 4) -> None:
        if n_quantiles < 2:
            raise ValueError("n_quantiles must be >= 2")
        if column not in source.headers:
            raise ValueError(f"Column '{column}' not found in source headers")
        self._source = source
        self._column = column
        self._n = n_quantiles
        self._built = False
        self._buckets: Dict[str, List[dict]] = {}

    @property
    def headers(self) -> List[str]:
        return list(self._source.headers)

    def _ensure_built(self) -> None:
        if self._built:
            return
        all_rows = list(self._source.rows())
        col = self._column
        numeric: List[tuple] = []
        other: List[dict] = []
        for row in all_rows:
            try:
                numeric.append((float(row[col]), row))
            except (ValueError, TypeError):
                other.append(row)

        numeric.sort(key=lambda t: t[0])
        total = len(numeric)
        self._buckets = {f"Q{i + 1}": [] for i in range(self._n)}
        if other:
            self._buckets["Q_other"] = other
        for idx, (_, row) in enumerate(numeric):
            bucket_idx = min(int(idx * self._n / total), self._n - 1) if total else 0
            self._buckets[f"Q{bucket_idx + 1}"].append(row)
        self._built = True

    @property
    def group_keys(self) -> List[str]:
        self._ensure_built()
        return [k for k in self._buckets if self._buckets[k]]

    def rows(self, key: str) -> Iterator[dict]:
        self._ensure_built()
        yield from self._buckets.get(key, [])

    def row_count(self, key: str) -> int:
        self._ensure_built()
        return len(self._buckets.get(key, []))
