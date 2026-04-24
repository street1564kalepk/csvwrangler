"""CSVCompactor – remove columns whose values are all empty/whitespace."""
from __future__ import annotations

from typing import Iterable, Iterator


class CSVCompactor:
    """Drop columns that contain only empty or whitespace-only values.

    Parameters
    ----------
    source:
        Any object exposing ``headers`` (list[str]) and ``rows``
        (Iterable[dict]).
    keep_if_any:
        When *True* (default) a column is kept if **at least one** row has a
        non-empty value.  When *False* a column is kept only if **every** row
        has a non-empty value.
    """

    def __init__(self, source, *, keep_if_any: bool = True) -> None:
        self._source = source
        self._keep_if_any = keep_if_any
        self._headers: list[str] | None = None
        self._data: list[dict] | None = None

    # ------------------------------------------------------------------
    def _build(self) -> None:
        if self._data is not None:
            return

        raw: list[dict] = list(self._source.rows)
        src_headers: list[str] = list(self._source.headers)

        if not raw:
            # No rows – keep all headers, emit nothing.
            self._headers = src_headers
            self._data = []
            return

        def _non_empty(val: object) -> bool:
            return bool(str(val).strip()) if val is not None else False

        if self._keep_if_any:
            kept = [
                h for h in src_headers
                if any(_non_empty(row.get(h, "")) for row in raw)
            ]
        else:
            kept = [
                h for h in src_headers
                if all(_non_empty(row.get(h, "")) for row in raw)
            ]

        self._headers = kept
        self._data = [{h: row.get(h, "") for h in kept} for row in raw]

    # ------------------------------------------------------------------
    @property
    def headers(self) -> list[str]:
        self._build()
        return list(self._headers)  # type: ignore[arg-type]

    @property
    def rows(self) -> Iterator[dict]:
        self._build()
        yield from self._data  # type: ignore[union-attr]

    @property
    def row_count(self) -> int:
        self._build()
        return len(self._data)  # type: ignore[arg-type]
