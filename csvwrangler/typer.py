"""CSVTyper: auto-detect and cast column types based on data inspection."""
from __future__ import annotations
from typing import Iterator


class CSVTyper:
    """Inspect rows and cast columns to detected types (int, float, or str)."""

    SUPPORTED = ("int", "float", "str")

    def __init__(self, source, sample_size: int = 100):
        self._source = source
        self._sample_size = sample_size
        self._detected: dict[str, str] = {}
        self._detect()

    def _detect(self) -> None:
        sample = []
        for i, row in enumerate(self._source.rows()):
            if i >= self._sample_size:
                break
            sample.append(row)

        if not sample:
            self._detected = {h: "str" for h in self._source.headers()}
            return

        for col in self._source.headers():
            values = [r[col] for r in sample if r.get(col, "").strip() != ""]
            self._detected[col] = self._infer(values)

    def _infer(self, values: list[str]) -> str:
        if not values:
            return "str"
        for t, fn in (("int", int), ("float", float)):
            try:
                for v in values:
                    fn(v)
                return t
            except (ValueError, TypeError):
                continue
        return "str"

    def detected_types(self) -> dict[str, str]:
        return dict(self._detected)

    def headers(self) -> list[str]:
        return self._source.headers()

    def rows(self) -> Iterator[dict]:
        casters = {
            "int": int,
            "float": float,
            "str": str,
        }
        for row in self._source.rows():
            out = {}
            for col in self.headers():
                raw = row.get(col, "")
                t = self._detected.get(col, "str")
                try:
                    out[col] = casters[t](raw) if raw.strip() != "" else raw
                except (ValueError, TypeError):
                    out[col] = raw
            yield out

    def row_count(self) -> int:
        return sum(1 for _ in self.rows())
