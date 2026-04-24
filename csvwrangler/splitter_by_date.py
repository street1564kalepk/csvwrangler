"""Split CSV rows into groups based on a date column and a truncation period."""
from __future__ import annotations

import re
from collections import defaultdict
from typing import Dict, Iterator, List


_PERIODS = ("year", "month", "week", "day")


def _truncate(value: str, period: str) -> str:
    """Return a grouping key by truncating an ISO-8601 date string."""
    # Accept YYYY-MM-DD (or YYYY-MM-DDTHH:MM:SS …)
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", value.strip())
    if not m:
        return "unknown"
    year, month, day = m.group(1), m.group(2), m.group(3)
    if period == "year":
        return year
    if period == "month":
        return f"{year}-{month}"
    if period == "day":
        return f"{year}-{month}-{day}"
    if period == "week":
        import datetime
        try:
            d = datetime.date(int(year), int(month), int(day))
            iso = d.isocalendar()
            return f"{iso[0]}-W{iso[1]:02d}"
        except ValueError:
            return "unknown"
    return value


class CSVDateSplitter:
    """Split a CSV source into per-period buckets keyed by truncated date."""

    def __init__(self, source, column: str, period: str = "month"):
        if period not in _PERIODS:
            raise ValueError(f"period must be one of {_PERIODS}; got {period!r}")
        self._source = source
        self._column = column
        self._period = period
        self._built: Dict[str, List[dict]] | None = None

    # ------------------------------------------------------------------
    def headers(self) -> List[str]:
        return list(self._source.headers())

    def _ensure_built(self) -> None:
        if self._built is not None:
            return
        col = self._column
        hdrs = self.headers()
        if col not in hdrs:
            raise KeyError(f"Column {col!r} not found in headers {hdrs}")
        buckets: Dict[str, List[dict]] = defaultdict(list)
        for row in self._source.rows():
            key = _truncate(row.get(col, ""), self._period)
            buckets[key].append(row)
        self._built = dict(buckets)

    def group_keys(self) -> List[str]:
        self._ensure_built()
        return sorted(self._built.keys())  # type: ignore[union-attr]

    def group_count(self) -> int:
        return len(self.group_keys())

    def rows_for(self, key: str) -> Iterator[dict]:
        self._ensure_built()
        yield from self._built.get(key, [])  # type: ignore[union-attr]

    def row_count_for(self, key: str) -> int:
        self._ensure_built()
        return len(self._built.get(key, []))  # type: ignore[union-attr]
