import csv
import io
from typing import Iterator, List, Optional

from csvwrangler.reader import CSVReader
from csvwrangler.filter import CSVFilter
from csvwrangler.transform import CSVTransform
from csvwrangler.writer import CSVWriter


class Pipeline:
    """
    Chainable pipeline for reading, filtering, transforming, and writing CSV data.
    """

    def __init__(self, source: CSVReader):
        self._source = source
        self._stage = source  # current active stage

    def select(self, *columns: str) -> "Pipeline":
        """Keep only the specified columns in the output."""
        outer = self

        class _Projection:
            @property
            def headers(self) -> List[str]:
                return [c for c in columns if c in outer._stage.headers]

            def rows(self) -> Iterator[dict]:
                keep = self.headers
                for row in outer._stage.rows():
                    yield {k: row[k] for k in keep if k in row}

        self._stage = _Projection()
        return self

    def filter(self, column: str, op: str, value: str) -> "Pipeline":
        """Filter rows using a CSVFilter predicate."""
        f = CSVFilter(self._stage)
        f.where(column, op, value)
        self._stage = f
        return self

    def transform(self) -> CSVTransform:
        """
        Attach a CSVTransform stage and return it so the caller can
        chain rename / add_column / apply calls, then pass back to pipeline
        via continue_with().
        """
        t = CSVTransform(self._stage)
        self._stage = t
        return t

    def continue_with(self, stage) -> "Pipeline":
        """Attach an externally built stage (e.g. CSVTransform) to the pipeline."""
        self._stage = stage
        return self

    def to_file(self, path: str) -> None:
        """Write the pipeline output to a CSV file."""
        writer = CSVWriter(self._stage.headers)
        writer.write(self._stage.rows(), path)

    def to_string(self) -> str:
        """Return the pipeline output as a CSV-formatted string."""
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=self._stage.headers)
        writer.writeheader()
        for row in self._stage.rows():
            writer.writerow(row)
        return buf.getvalue()

    def __repr__(self) -> str:
        return f"Pipeline(stage={self._stage!r})"
