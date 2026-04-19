from __future__ import annotations
from typing import Iterable, Dict, Any, List


class CSVSummarizer:
    """Produce a per-column statistical summary (count, min, max, mean, unique)."""

    def __init__(self, source) -> None:
        self._source = source
        self._summary: Dict[str, Dict[str, Any]] | None = None

    @property
    def headers(self) -> List[str]:
        return ["column", "count", "unique", "min", "max", "mean"]

    def _build(self) -> None:
        if self._summary is not None:
            return
        accum: Dict[str, Dict[str, Any]] = {
            h: {"values": [], "numeric": []} for h in self._source.headers
        }
        for row in self._source.rows():
            for h in self._source.headers:
                val = row.get(h, "")
                accum[h]["values"].append(val)
                try:
                    accum[h]["numeric"].append(float(val))
                except (ValueError, TypeError):
                    pass
        self._summary = {}
        for h, data in accum.items():
            nums = data["numeric"]
            vals = data["values"]
            self._summary[h] = {
                "count": len(vals),
                "unique": len(set(vals)),
                "min": min(nums) if nums else "",
                "max": max(nums) if nums else "",
                "mean": round(sum(nums) / len(nums), 6) if nums else "",
            }

    def rows(self) -> Iterable[Dict[str, Any]]:
        self._build()
        for col, stats in self._summary.items():
            yield {
                "column": col,
                "count": stats["count"],
                "unique": stats["unique"],
                "min": stats["min"],
                "max": stats["max"],
                "mean": stats["mean"],
            }

    @property
    def row_count(self) -> int:
        return len(self._source.headers)
