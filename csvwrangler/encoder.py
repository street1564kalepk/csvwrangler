"""CSVEncoder: encode/decode column values using base64 or url encoding."""
import base64
import urllib.parse
from typing import Iterable


SUPPORTED = {"base64", "url"}


class CSVEncoder:
    """Encode or decode columns in a CSV stream."""

    def __init__(self, source, columns: list[str], mode: str = "encode", encoding: str = "base64"):
        if encoding not in SUPPORTED:
            raise ValueError(f"Unsupported encoding '{encoding}'. Choose from {SUPPORTED}.")
        if mode not in ("encode", "decode"):
            raise ValueError(f"mode must be 'encode' or 'decode', got '{mode}'.")
        self._source = source
        self._columns = columns
        self._mode = mode
        self._encoding = encoding

    def headers(self) -> list[str]:
        return self._source.headers()

    def _transform(self, value: str) -> str:
        if self._encoding == "base64":
            if self._mode == "encode":
                return base64.b64encode(value.encode()).decode()
            else:
                try:
                    return base64.b64decode(value.encode()).decode()
                except Exception:
                    return value
        else:  # url
            if self._mode == "encode":
                return urllib.parse.quote(value)
            else:
                return urllib.parse.unquote(value)

    def rows(self) -> Iterable[dict]:
        hdrs = self.headers()
        for row in self._source.rows():
            new_row = dict(row)
            for col in self._columns:
                if col in hdrs and col in new_row:
                    new_row[col] = self._transform(new_row[col])
            yield new_row

    def row_count(self) -> int:
        return sum(1 for _ in self.rows())
