"""CSV writer module for outputting transformed data."""

import csv
import sys
from pathlib import Path
from typing import Iterable, Optional


class CSVWriter:
    """Writes rows to a CSV file or stdout."""

    def __init__(
        self,
        filepath: Optional[str] = None,
        delimiter: str = ",",
        encoding: str = "utf-8",
    ):
        self.filepath = Path(filepath) if filepath else None
        self.delimiter = delimiter
        self.encoding = encoding

    def write(self, rows: Iterable[dict], headers: Optional[list[str]] = None) -> int:
        """Write rows to the configured output. Returns the number of rows written."""
        row_iter = iter(rows)
        first_row = next(row_iter, None)
        if first_row is None:
            return 0

        fieldnames = headers or list(first_row.keys())
        count = 0

        def _write_to(f):
            nonlocal count
            writer = csv.DictWriter(
                f,
                fieldnames=fieldnames,
                delimiter=self.delimiter,
                extrasaction="ignore",
                lineterminator="\n",
            )
            writer.writeheader()
            writer.writerow(first_row)
            count += 1
            for row in row_iter:
                writer.writerow(row)
                count += 1

        if self.filepath:
            self.filepath.parent.mkdir(parents=True, exist_ok=True)
            with open(self.filepath, "w", encoding=self.encoding, newline="") as f:
                _write_to(f)
        else:
            _write_to(sys.stdout)

        return count

    def __repr__(self) -> str:
        target = str(self.filepath) if self.filepath else "stdout"
        return f"CSVWriter(target={target!r}, delimiter={self.delimiter!r})"
