"""CSVBooleanSplitter – splits rows into two groups based on a boolean-like column value."""

from __future__ import annotations

_TRUTHY = {"true", "1", "yes", "y", "on"}
_FALSY = {"false", "0", "no", "n", "off", ""}


class CSVBooleanSplitter:
    """Split a CSV source into 'true' and 'false' groups based on a column's boolean value."""

    def __init__(self, source, column: str, *, strict: bool = False):
        self._source = source
        self._column = column
        self._strict = strict
        self._true_rows: list[dict] | None = None
        self._false_rows: list[dict] | None = None

    @property
    def headers(self) -> list[str]:
        return list(self._source.headers)

    def _ensure_built(self) -> None:
        if self._true_rows is not None:
            return
        col = self._column
        if col not in self._source.headers:
            raise KeyError(f"Column '{col}' not found in headers")
        true_rows: list[dict] = []
        false_rows: list[dict] = []
        for row in self._source.rows:
            raw = str(row.get(col, "")).strip().lower()
            if raw in _TRUTHY:
                true_rows.append(row)
            elif raw in _FALSY or not self._strict:
                false_rows.append(row)
            else:
                raise ValueError(
                    f"Unrecognised boolean value '{row.get(col)}' in column '{col}'"
                )
        self._true_rows = true_rows
        self._false_rows = false_rows

    @property
    def true_rows(self) -> list[dict]:
        self._ensure_built()
        return self._true_rows  # type: ignore[return-value]

    @property
    def false_rows(self) -> list[dict]:
        self._ensure_built()
        return self._false_rows  # type: ignore[return-value]

    @property
    def true_count(self) -> int:
        return len(self.true_rows)

    @property
    def false_count(self) -> int:
        return len(self.false_rows)
