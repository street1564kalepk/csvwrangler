"""CSVTransposer: pivot rows into columns (transpose a CSV table)."""
from typing import Iterator


class CSVTransposer:
    """Transpose a CSV source so rows become columns and vice-versa.

    The first column of the output contains the original header names.
    Each subsequent column corresponds to one original data row, labelled
    row_1, row_2, … (or a custom prefix).
    """

    def __init__(self, source, row_label_column: str = "field", row_prefix: str = "row"):
        self._source = source
        self._label_col = row_label_column
        self._prefix = row_prefix
        self._data: list[dict] | None = None

    def _build(self) -> None:
        if self._data is not None:
            return
        src_headers = self._source.headers
        src_rows = list(self._source.rows())
        n = len(src_rows)
        row_labels = [f"{self._prefix}_{i + 1}" for i in range(n)]
        result = []
        for col in src_headers:
            record = {self._label_col: col}
            for label, row in zip(row_labels, src_rows):
                record[label] = row.get(col, "")
            result.append(record)
        self._data = result
        self._row_labels = row_labels

    @property
    def headers(self) -> list[str]:
        self._build()
        return [self._label_col] + self._row_labels

    def rows(self) -> Iterator[dict]:
        self._build()
        yield from self._data

    def row_count(self) -> int:
        self._build()
        return len(self._data)

    def __repr__(self) -> str:  # pragma: no cover
        return f"CSVTransposer(label_col={self._label_col!r}, prefix={self._prefix!r})"
