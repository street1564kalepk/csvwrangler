import csv
import io
from typing import List, Optional, Dict

from csvwrangler.reader import CSVReader
from csvwrangler.filter import CSVFilter
from csvwrangler.transform import CSVTransform
from csvwrangler.sorter import CSVSorter
from csvwrangler.deduplicator import CSVDeduplicator
from csvwrangler.aggregator import CSVAggregator
from csvwrangler.writer import CSVWriter


class Pipeline:
    """Chainable pipeline for CSV processing."""

    def __init__(self, source):
        self._source = source

    # -- projection ----------------------------------------------------------

    def select(self, *columns: str) -> "Pipeline":
        return Pipeline(_Projection(self._source, list(columns)))

    # -- filtering -----------------------------------------------------------

    def where(self, column: str, op: str, value: str) -> "Pipeline":
        f = CSVFilter(self._source)
        f.where(column, op, value)
        return Pipeline(f)

    # -- transforming --------------------------------------------------------

    def rename(self, old: str, new: str) -> "Pipeline":
        t = CSVTransform(self._source)
        t.rename(old, new)
        return Pipeline(t)

    def add_column(self, name: str, func) -> "Pipeline":
        t = CSVTransform(self._source)
        t.add_column(name, func)
        return Pipeline(t)

    # -- sorting -------------------------------------------------------------

    def sort(self, column: str, descending: bool = False) -> "Pipeline":
        return Pipeline(CSVSorter(self._source, column, descending=descending))

    # -- deduplication -------------------------------------------------------

    def deduplicate(self, *columns: str) -> "Pipeline":
        cols = list(columns) if columns else None
        return Pipeline(CSVDeduplicator(self._source, columns=cols))

    # -- aggregation ---------------------------------------------------------

    def aggregate(self, group_by: List[str], aggregations: Dict[str, str]) -> "Pipeline":
        return Pipeline(CSVAggregator(self._source, group_by, aggregations))

    # -- terminal operations -------------------------------------------------

    def to_file(self, path: str) -> None:
        writer = CSVWriter(self._source)
        writer.write(path)

    def to_string(self) -> str:
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=self._source.headers)
        writer.writeheader()
        for row in self._source.rows():
            writer.writerow(row)
        return buf.getvalue()

    @property
    def headers(self):
        return self._source.headers


class _Projection:
    def __init__(self, source, columns: List[str]):
        self._source = source
        self._columns = columns

    @property
    def headers(self) -> List[str]:
        return self._columns

    def rows(self):
        for row in self._source.rows():
            yield {c: row[c] for c in self._columns}

    def row_count(self) -> int:
        return sum(1 for _ in self.rows())
