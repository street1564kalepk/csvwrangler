"""CSVSequencer – add an auto-incrementing sequence number column."""

from __future__ import annotations
from typing import Iterator


class CSVSequencer:
    """Prepend or append a sequence-number column to every row."""

    def __init__(
        self,
        source,
        column: str = "seq",
        start: int = 1,
        step: int = 1,
        position: str = "first",  # "first" | "last"
    ) -> None:
        if position not in ("first", "last"):
            raise ValueError("position must be 'first' or 'last'")
        self._source = source
        self._column = column
        self._start = start
        self._step = step
        self._position = position

    # ------------------------------------------------------------------
    @property
    def headers(self) -> list[str]:
        src = list(self._source.headers)
        if self._position == "first":
            return [self._column] + src
        return src + [self._column]

    def rows(self) -> Iterator[dict]:
        counter = self._start
        for row in self._source.rows():
            seq_val = str(counter)
            if self._position == "first":
                yield {self._column: seq_val, **row}
            else:
                yield {**row, self._column: seq_val}
            counter += self._step

    @property
    def row_count(self) -> int:
        return sum(1 for _ in self.rows())
