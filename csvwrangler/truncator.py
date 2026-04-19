"""CSVTruncator – truncate string values in specified columns to a max length."""

from __future__ import annotations
from typing import Iterable, Iterator


class CSVTruncator:
    """Truncate string values in named columns to *max_len* characters.

    Parameters
    ----------
    source:
        Any object exposing ``.headers`` and ``.rows``.
    columns:
        Mapping of ``{column_name: max_length}``.
    ellipsis_str:
        String appended when a value is truncated (default ``"…"``).
    """

    def __init__(
        self,
        source,
        columns: dict[str, int],
        ellipsis_str: str = "…",
    ) -> None:
        unknown = set(columns) - set(source.headers)
        if unknown:
            raise ValueError(f"Unknown columns: {sorted(unknown)}")
        for col, length in columns.items():
            if not isinstance(length, int) or length < 1:
                raise ValueError(
                    f"max_len for '{col}' must be a positive integer, got {length!r}"
                )
        self._source = source
        self._columns = columns
        self._ellipsis = ellipsis_str

    @property
    def headers(self) -> list[str]:
        return list(self._source.headers)

    def rows(self) -> Iterator[dict]:
        cols = self._columns
        ell = self._ellipsis
        for row in self._source.rows():
            out = dict(row)
            for col, max_len in cols.items():
                val = str(out.get(col, ""))
                if len(val) > max_len:
                    out[col] = val[:max_len] + ell
            yield out

    def row_count(self) -> int:
        return sum(1 for _ in self.rows())
