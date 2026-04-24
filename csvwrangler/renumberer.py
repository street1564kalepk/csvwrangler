"""CSVRenumberer – reset or resequence a numeric column.

Given a source, a target column name, a start value, and a step,
replaces every value in that column with start, start+step, start+2*step …
The column must already exist in the source headers.
"""

from __future__ import annotations

from typing import Iterable, Iterator


class CSVRenumberer:
    """Resequence a single numeric column across all rows."""

    def __init__(
        self,
        source,
        column: str,
        start: int | float = 1,
        step: int | float = 1,
    ) -> None:
        if column not in source.headers:
            raise ValueError(
                f"Column '{column}' not found in headers: {source.headers}"
            )
        if step == 0:
            raise ValueError("step must not be zero")
        self._source = source
        self._column = column
        self._start = start
        self._step = step

    @property
    def headers(self) -> list[str]:
        return list(self._source.headers)

    def rows(self) -> Iterator[dict]:
        current = self._start
        for row in self._source.rows():
            new_row = dict(row)
            new_row[self._column] = current
            current += self._step
            yield new_row

    @property
    def row_count(self) -> int:
        return sum(1 for _ in self.rows())

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"CSVRenumberer(column={self._column!r}, "
            f"start={self._start}, step={self._step})"
        )
