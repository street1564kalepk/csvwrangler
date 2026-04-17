"""CSVFormatter – apply printf-style or Python format strings to columns."""
from __future__ import annotations
from typing import Iterable, Dict, Any


class CSVFormatter:
    """Apply format templates to one or more columns.

    Parameters
    ----------
    source:
        Any object exposing ``.headers`` and ``.rows``.
    formats:
        Mapping of column name -> format string.
        The format string may reference the column value as ``{value}``
        or any other column in the row by name, e.g. ``{name} ({age})``.

    Example
    -------
    formatter = CSVFormatter(source, {"full_name": "{first} {last}"})
    """

    def __init__(self, source, formats: Dict[str, str]) -> None:
        self._source = source
        self._formats = formats
        unknown = set(formats) - set(source.headers)
        if unknown:
            raise ValueError(f"Unknown columns in formats: {sorted(unknown)}")

    # ------------------------------------------------------------------
    @property
    def headers(self):
        return self._source.headers

    # ------------------------------------------------------------------
    def rows(self) -> Iterable[Dict[str, Any]]:
        for row in self._source.rows():
            out = dict(row)
            for col, tmpl in self._formats.items():
                try:
                    out[col] = tmpl.format(value=row[col], **row)
                except (KeyError, ValueError) as exc:
                    raise ValueError(
                        f"Format error for column '{col}' with template '{tmpl}': {exc}"
                    ) from exc
            yield out

    # ------------------------------------------------------------------
    @property
    def row_count(self) -> int:
        return sum(1 for _ in self.rows())

    def __repr__(self) -> str:  # pragma: no cover
        return f"CSVFormatter(formats={list(self._formats)})"
