"""CSVStripper – remove leading/trailing whitespace from string values.

Supports targeting specific columns or all columns.
"""
from __future__ import annotations

from typing import Iterable, Iterator, List, Optional


class CSVStripper:
    """Strips whitespace from string cell values in chosen columns.

    Parameters
    ----------
    source:
        Any object exposing ``.headers`` (list[str]) and ``.rows``
        (iterable of dict[str, str]).
    columns:
        Column names to strip.  Pass ``None`` (default) to strip every
        column.
    chars:
        Characters to strip.  Defaults to ``None`` which strips any
        whitespace (mirrors :pymeth:`str.strip`).
    """

    def __init__(
        self,
        source,
        columns: Optional[List[str]] = None,
        chars: Optional[str] = None,
    ) -> None:
        self._source = source
        self._chars = chars

        all_headers: List[str] = list(source.headers)
        if columns is None:
            self._targets: List[str] = all_headers
        else:
            unknown = [c for c in columns if c not in all_headers]
            if unknown:
                raise ValueError(
                    f"CSVStripper: unknown column(s): {unknown}"
                )
            self._targets = list(columns)

    @property
    def headers(self) -> List[str]:
        return list(self._source.headers)

    def rows(self) -> Iterator[dict]:
        targets = set(self._targets)
        chars = self._chars
        for row in self._source.rows():
            yield {
                k: (v.strip(chars) if k in targets and isinstance(v, str) else v)
                for k, v in row.items()
            }

    @property
    def row_count(self) -> int:
        return sum(1 for _ in self.rows())
