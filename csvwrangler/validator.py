"""CSVValidator: validate rows against column rules."""
from typing import Iterable


class CSVValidator:
    """Validate rows, optionally dropping or tagging invalid ones."""

    MODES = ("drop", "tag", "raise")

    def __init__(self, source, rules: dict, mode: str = "drop", tag_column: str = "_errors"):
        """
        source   – upstream source with .headers and .rows()
        rules    – {column: callable} where callable returns True if valid
        mode     – 'drop' | 'tag' | 'raise'
        tag_column – column name used when mode='tag'
        """
        if mode not in self.MODES:
            raise ValueError(f"mode must be one of {self.MODES}, got {mode!r}")
        unknown = set(rules) - set(source.headers)
        if unknown:
            raise ValueError(f"Unknown columns in rules: {unknown}")
        self._source = source
        self._rules = rules
        self._mode = mode
        self._tag_column = tag_column

    @property
    def headers(self):
        if self._mode == "tag":
            return self._source.headers + [self._tag_column]
        return self._source.headers

    def rows(self) -> Iterable[dict]:
        for row in self._source.rows():
            errors = [
                col for col, fn in self._rules.items() if not fn(row.get(col, ""))
            ]
            if not errors:
                if self._mode == "tag":
                    yield {**row, self._tag_column: ""}
                else:
                    yield row
            else:
                if self._mode == "drop":
                    continue
                elif self._mode == "tag":
                    yield {**row, self._tag_column: "|".join(errors)}
                elif self._mode == "raise":
                    raise ValueError(f"Validation failed for columns: {errors} in row {row}")

    def invalid_rows(self) -> Iterable[dict]:
        """Yield rows that fail validation, each annotated with a '_errors' key
        listing the failing column names regardless of the current mode.
        Useful for inspecting bad data without changing the validator's mode.
        """
        for row in self._source.rows():
            errors = [
                col for col, fn in self._rules.items() if not fn(row.get(col, ""))
            ]
            if errors:
                yield {**row, "_errors": "|".join(errors)}

    def row_count(self) -> int:
        return sum(1 for _ in self.rows())

    def __repr__(self):
        return f"CSVValidator(mode={self._mode!r}, rules={list(self._rules)!r})"
