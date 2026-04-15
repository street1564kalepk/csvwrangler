"""Pipeline — chainable builder that wires CSV components together."""
from __future__ import annotations

import io
from pathlib import Path
from typing import Callable

from csvwrangler.reader import CSVReader
from csvwrangler.writer import CSVWriter
from csvwrangler.filter import CSVFilter
from csvwrangler.transform import CSVTransform
from csvwrangler.sorter import CSVSorter
from csvwrangler.deduplicator import CSVDeduplicator
from csvwrangler.aggregator import CSVAggregator
from csvwrangler.limiter import CSVLimiter
from csvwrangler.slicer import CSVSlicer


class Pipeline:
    """Chainable pipeline over a CSV source."""

    def __init__(self, source) -> None:
        self._source = source

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------

    @classmethod
    def from_file(cls, path: str | Path, encoding: str = "utf-8") -> "Pipeline":
        return cls(CSVReader(path, encoding=encoding))

    # ------------------------------------------------------------------
    # Transformation steps
    # ------------------------------------------------------------------

    def select(self, *columns: str) -> "Pipeline":
        """Keep only the specified columns."""
        return Pipeline(CSVTransform(self._source).select(columns))

    def where(self, column: str, op: str, value: str) -> "Pipeline":
        return Pipeline(CSVFilter(self._source, column, op, value))

    def rename(self, **mapping: str) -> "Pipeline":
        return Pipeline(CSVTransform(self._source).rename(mapping))

    def add_column(self, name: str, fn: Callable[[dict], str]) -> "Pipeline":
        return Pipeline(CSVTransform(self._source).add_column(name, fn))

    def sort(self, column: str, ascending: bool = True) -> "Pipeline":
        return Pipeline(CSVSorter(self._source, column, ascending=ascending))

    def then_sort(self, column: str, ascending: bool = True) -> "Pipeline":
        if not isinstance(self._source, CSVSorter):
            raise TypeError("then_sort() must follow sort()")
        return Pipeline(self._source.then_sort(column, ascending=ascending))

    def deduplicate(self, *columns: str) -> "Pipeline":
        cols = list(columns) if columns else None
        return Pipeline(CSVDeduplicator(self._source, columns=cols))

    def aggregate(self, group_by: list[str], aggregations: list[tuple]) -> "Pipeline":
        return Pipeline(CSVAggregator(self._source, group_by=group_by, aggregations=aggregations))

    def limit(self, n: int) -> "Pipeline":
        return Pipeline(CSVLimiter(self._source, n))

    def skip(self, n: int) -> "Pipeline":
        """Skip the first *n* data rows."""
        return Pipeline(CSVSlicer(self._source, offset=n))

    def slice(self, offset: int = 0, limit: int | None = None) -> "Pipeline":
        """Skip *offset* rows then yield at most *limit* rows."""
        return Pipeline(CSVSlicer(self._source, offset=offset, limit=limit))

    # ------------------------------------------------------------------
    # Terminal steps
    # ------------------------------------------------------------------

    @property
    def headers(self) -> list[str]:
        return self._source.headers

    def rows(self):
        return self._source.rows()

    def to_file(self, path: str | Path, encoding: str = "utf-8") -> None:
        CSVWriter(self._source).write(path, encoding=encoding)

    def to_string(self) -> str:
        buf = io.StringIO()
        CSVWriter(self._source)._write_to(buf)
        return buf.getvalue()

    def __repr__(self) -> str:  # pragma: no cover
        return f"Pipeline(source={self._source!r})"
