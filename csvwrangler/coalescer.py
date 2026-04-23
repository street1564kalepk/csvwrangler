"""CSVCoalescer: fills null/empty values in a column using the first
non-empty value found across a prioritised list of fallback columns."""

from typing import List


class CSVCoalescer:
    """Return the first non-empty value across *columns* for each row,
    writing the result into *target*.

    Parameters
    ----------
    source      : any object exposing .headers and .rows
    columns     : ordered list of column names to coalesce (first wins)
    target      : name of the output column (appended if new, replaced if
                  it already exists in *columns*)
    empty_values: values treated as "empty" (default: {"", None})
    """

    def __init__(
        self,
        source,
        columns: List[str],
        target: str,
        empty_values=None,
    ):
        if not columns:
            raise ValueError("columns must not be empty")
        src_headers = list(source.headers)
        for col in columns:
            if col not in src_headers:
                raise ValueError(f"Column '{col}' not found in source")

        self._source = source
        self._columns = columns
        self._target = target
        self._empty = {"" , None} if empty_values is None else set(empty_values)

        if target in src_headers:
            self._out_headers = src_headers
        else:
            self._out_headers = src_headers + [target]

    @property
    def headers(self) -> List[str]:
        return list(self._out_headers)

    def _coalesce(self, row: dict):
        for col in self._columns:
            val = row.get(col)
            if val not in self._empty:
                return val
        return ""

    def rows(self):
        for row in self._source.rows():
            out = dict(row)
            out[self._target] = self._coalesce(row)
            yield out

    @property
    def row_count(self) -> int:
        return sum(1 for _ in self.rows())
