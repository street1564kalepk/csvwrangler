"""CSVProfiler – compute basic column statistics for a CSV source."""
from __future__ import annotations
from typing import Any


class CSVProfiler:
    """Compute per-column statistics (count, nulls, min, max, unique)."""

    def __init__(self, source: Any) -> None:
        self._source = source
        self._profile: dict[str, dict] | None = None

    # ------------------------------------------------------------------
    def _build(self) -> None:
        if self._profile is not None:
            return
        hdrs = self._source.headers
        stats: dict[str, dict] = {
            h: {"count": 0, "nulls": 0, "min": None, "max": None, "unique": set()}
            for h in hdrs
        }
        for row in self._source.rows:
            for h in hdrs:
                val = row.get(h, "")
                s = stats[h]
                s["count"] += 1
                if val == "" or val is None:
                    s["nulls"] += 1
                else:
                    s["unique"].add(val)
                    try:
                        num = float(val)
                        s["min"] = num if s["min"] is None else min(s["min"], num)
                        s["max"] = num if s["max"] is None else max(s["max"], num)
                    except (ValueError, TypeError):
                        s["min"] = val if s["min"] is None else min(str(s["min"]), str(val))
                        s["max"] = val if s["max"] is None else max(str(s["max"]), str(val))
        # convert sets to counts
        for h in hdrs:
            stats[h]["unique"] = len(stats[h]["unique"])
        self._profile = stats

    # ------------------------------------------------------------------
    @property
    def headers(self) -> list[str]:
        return self._source.headers

    def profile(self) -> dict[str, dict]:
        """Return the full profile dict keyed by column name."""
        self._build()
        return self._profile  # type: ignore[return-value]

    def column(self, name: str) -> dict:
        """Return stats for a single column."""
        self._build()
        if name not in self._profile:  # type: ignore[operator]
            raise KeyError(f"Column '{name}' not found")
        return self._profile[name]  # type: ignore[index]

    def summary_rows(self) -> list[dict]:
        """Return profile as a list of dicts suitable for CSV output."""
        self._build()
        out = []
        for col, s in self._profile.items():  # type: ignore[union-attr]
            out.append({
                "column": col,
                "count": s["count"],
                "nulls": s["nulls"],
                "unique": s["unique"],
                "min": "" if s["min"] is None else s["min"],
                "max": "" if s["max"] is None else s["max"],
            })
        return out
