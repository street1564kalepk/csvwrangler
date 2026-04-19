"""CSVDivider – split a numeric column by a divisor and store the result."""
from __future__ import annotations
from typing import Iterable, Iterator


class CSVDivider:
    """Divide one or more numeric columns by a constant divisor.

    Parameters
    ----------
    source      : any object exposing .headers and .rows
    columns     : list of column names to divide
    divisor     : number to divide by (non-zero)
    precision   : decimal places to round to (None = no rounding)
    suffix      : appended to the new column name; empty string = in-place
    """

    def __init__(
        self,
        source,
        columns: list[str],
        divisor: float,
        *,
        precision: int | None = None,
        suffix: str = "",
    ) -> None:
        if divisor == 0:
            raise ValueError("divisor must be non-zero")
        unknown = [c for c in columns if c not in source.headers]
        if unknown:
            raise ValueError(f"unknown columns: {unknown}")
        self._source = source
        self._columns = columns
        self._divisor = divisor
        self._precision = precision
        self._suffix = suffix
        # build output headers
        if suffix:
            extra = [f"{c}{suffix}" for c in columns]
            self._headers = source.headers + extra
        else:
            self._headers = list(source.headers)

    @property
    def headers(self) -> list[str]:
        return list(self._headers)

    def rows(self) -> Iterator[dict]:
        for row in self._source.rows():
            out = dict(row)
            for col in self._columns:
                raw = row.get(col, "")
                try:
                    result = float(raw) / self._divisor
                    if self._precision is not None:
                        result = round(result, self._precision)
                    value = str(result)
                except (ValueError, TypeError):
                    value = ""
                target = f"{col}{self._suffix}" if self._suffix else col
                out[target] = value
            yield {h: out.get(h, "") for h in self._headers}

    @property
    def row_count(self) -> int:
        return sum(1 for _ in self.rows())
