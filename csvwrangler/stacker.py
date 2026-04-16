"""CSVStacker: vertically stack (union) multiple CSV sources."""

from typing import Iterator


class CSVStacker:
    """Stack multiple CSV sources row-by-row (like SQL UNION ALL).

    All sources must share the same headers. Missing columns in a source
    are filled with an empty string.
    """

    def __init__(self, *sources):
        if not sources:
            raise ValueError("CSVStacker requires at least one source.")
        self._sources = sources
        self._headers = list(sources[0].headers)
        # Validate all sources have the same headers
        for src in sources[1:]:
            if list(src.headers) != self._headers:
                raise ValueError(
                    f"All sources must share the same headers. "
                    f"Expected {self._headers}, got {list(src.headers)}"
                )

    @property
    def headers(self) -> list:
        return self._headers

    def rows(self) -> Iterator[dict]:
        for source in self._sources:
            for row in source.rows():
                yield {col: row.get(col, "") for col in self._headers}

    @property
    def row_count(self) -> int:
        return sum(1 for _ in self.rows())

    def __repr__(self) -> str:  # pragma: no cover
        return f"CSVStacker(sources={len(self._sources)}, headers={self._headers})"
