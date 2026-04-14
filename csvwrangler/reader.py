"""CSV reader module with lazy/streaming support for large files."""

import csv
from pathlib import Path
from typing import Generator, Iterator, Optional


class CSVReader:
    """Lazily reads a CSV file row by row to handle large files efficiently."""

    def __init__(self, filepath: str, delimiter: str = ",", encoding: str = "utf-8"):
        self.filepath = Path(filepath)
        self.delimiter = delimiter
        self.encoding = encoding
        self._headers: Optional[list[str]] = None

        if not self.filepath.exists():
            raise FileNotFoundError(f"File not found: {self.filepath}")
        if not self.filepath.is_file():
            raise ValueError(f"Path is not a file: {self.filepath}")

    @property
    def headers(self) -> list[str]:
        """Return the header row of the CSV file."""
        if self._headers is None:
            with open(self.filepath, encoding=self.encoding, newline="") as f:
                reader = csv.reader(f, delimiter=self.delimiter)
                self._headers = next(reader, [])
        return self._headers

    def rows(self) -> Generator[dict, None, None]:
        """Yield each data row as a dict keyed by header names."""
        with open(self.filepath, encoding=self.encoding, newline="") as f:
            reader = csv.DictReader(f, delimiter=self.delimiter)
            for row in reader:
                yield dict(row)

    def row_count(self) -> int:
        """Count total data rows (excludes header). Reads entire file."""
        count = 0
        for _ in self.rows():
            count += 1
        return count

    def __repr__(self) -> str:
        return f"CSVReader(filepath={self.filepath!r}, delimiter={self.delimiter!r})"
