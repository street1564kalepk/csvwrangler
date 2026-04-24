"""CSVBucketer – assign rows to named buckets based on numeric range thresholds."""
from __future__ import annotations

from typing import Dict, Iterable, List, Tuple


class CSVBucketer:
    """Assign each row a bucket label based on the value of *column*.

    Parameters
    ----------
    source:
        Any object exposing ``.headers`` and ``.rows``.
    column:
        The column whose numeric value is evaluated.
    buckets:
        An ordered list of ``(label, upper_bound)`` pairs where *upper_bound*
        is exclusive.  Rows whose value exceeds every bound fall into the
        *default* bucket.
    target_column:
        Name of the new column appended to every output row.
    default:
        Label used when no bucket matches (default ``"other"``).
    """

    def __init__(
        self,
        source,
        column: str,
        buckets: List[Tuple[str, float]],
        target_column: str = "bucket",
        default: str = "other",
    ) -> None:
        if not buckets:
            raise ValueError("buckets must not be empty")
        if column not in source.headers:
            raise KeyError(f"column '{column}' not found in source headers")
        self._source = source
        self._column = column
        self._buckets = sorted(buckets, key=lambda x: x[1])
        self._target = target_column
        self._default = default

    # ------------------------------------------------------------------
    @property
    def headers(self) -> List[str]:
        return self._source.headers + [self._target]

    # ------------------------------------------------------------------
    def _label(self, raw: str) -> str:
        try:
            value = float(raw)
        except (ValueError, TypeError):
            return self._default
        for label, upper in self._buckets:
            if value < upper:
                return label
        return self._default

    # ------------------------------------------------------------------
    def rows(self) -> Iterable[Dict[str, str]]:
        for row in self._source.rows():
            label = self._label(row.get(self._column, ""))
            yield {**row, self._target: label}

    # ------------------------------------------------------------------
    @property
    def row_count(self) -> int:
        return sum(1 for _ in self.rows())
