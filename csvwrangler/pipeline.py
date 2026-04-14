from typing import Optional, List
from csvwrangler.reader import CSVReader
from csvwrangler.filter import CSVFilter
from csvwrangler.writer import CSVWriter


class Pipeline:
    """
    Chainable pipeline that connects a CSVReader -> CSVFilter -> CSVWriter.
    """

    def __init__(self, source: str, delimiter: str = ",", encoding: str = "utf-8"):
        self._reader = CSVReader(source, delimiter=delimiter, encoding=encoding)
        self._filter: Optional[CSVFilter] = None
        self._selected_columns: Optional[List[str]] = None

    def select(self, *columns: str) -> "Pipeline":
        """Restrict output to the specified columns."""
        for col in columns:
            if col not in self._reader.headers:
                raise ValueError(f"Column '{col}' not found in source headers.")
        self._selected_columns = list(columns)
        return self

    def filter(self) -> CSVFilter:
        """Return a CSVFilter attached to this pipeline for chaining conditions."""
        self._filter = CSVFilter(
            rows=self._reader.rows(),
            headers=self._reader.headers,
        )
        return self._filter

    def to_file(self, destination: str, delimiter: str = ",") -> int:
        """Write the (optionally filtered) rows to a CSV file. Returns rows written."""
        if self._filter is not None:
            row_iter = self._filter.apply()
        else:
            row_iter = self._reader.rows()

        headers = self._selected_columns or self._reader.headers

        def projected(rows):
            for row in rows:
                yield {col: row[col] for col in headers}

        writer = CSVWriter(destination, headers=headers, delimiter=delimiter)
        return writer.write(projected(row_iter))

    def to_stdout(self) -> int:
        """Write the (optionally filtered) rows to stdout. Returns rows written."""
        import sys
        if self._filter is not None:
            row_iter = self._filter.apply()
        else:
            row_iter = self._reader.rows()

        headers = self._selected_columns or self._reader.headers
        writer = CSVWriter(sys.stdout, headers=headers)
        return writer.write(row_iter)

    def __repr__(self) -> str:
        return (
            f"Pipeline(source={self._reader!r}, "
            f"filter={self._filter!r}, "
            f"columns={self._selected_columns})"
        )
