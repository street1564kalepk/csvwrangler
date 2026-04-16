"""CSVFiller: fill missing or empty values in specified columns."""

from typing import Any, Dict, Iterable, List, Optional


class CSVFiller:
    """Fills missing (empty string or None) values in specified columns
    with a constant value or the result of a callable.

    Parameters
    ----------
    source:
        Any object exposing ``.headers`` (list[str]) and ``.rows()``
        (iterable of dict).
    fill_values:
        Mapping of column name → fill value (str, int, float, etc.) or
        a zero-argument callable that returns the fill value.
    columns:
        If provided, only these columns are filled (must be a subset of
        *fill_values* keys).  Defaults to all keys in *fill_values*.
    """

    def __init__(
        self,
        source,
        fill_values: Dict[str, Any],
        columns: Optional[List[str]] = None,
    ) -> None:
        self._source = source
        self._fill_values = fill_values
        self._columns = columns if columns is not None else list(fill_values.keys())
        self._validate()

    def _validate(self) -> None:
        unknown = set(self._columns) - set(self._source.headers)
        if unknown:
            raise ValueError(
                f"CSVFiller: columns not found in source headers: {sorted(unknown)}"
            )
        missing_spec = set(self._columns) - set(self._fill_values.keys())
        if missing_spec:
            raise ValueError(
                f"CSVFiller: no fill value provided for columns: {sorted(missing_spec)}"
            )

    @property
    def headers(self) -> List[str]:
        return list(self._source.headers)

    def rows(self) -> Iterable[Dict[str, Any]]:
        fill_cols = set(self._columns)
        for row in self._source.rows():
            new_row = dict(row)
            for col in fill_cols:
                if new_row.get(col) in (None, ""):
                    spec = self._fill_values[col]
                    new_row[col] = spec() if callable(spec) else spec
            yield new_row

    def row_count(self) -> int:
        return sum(1 for _ in self.rows())

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"CSVFiller(columns={self._columns!r}, "
            f"fill_values={self._fill_values!r})"
        )
