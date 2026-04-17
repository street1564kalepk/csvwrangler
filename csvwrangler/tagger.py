"""CSVTagger – add a tag/label column based on conditional rules."""
from __future__ import annotations
from typing import Callable, Iterable


class CSVTagger:
    """Adds a new column whose value is determined by the first matching rule.

    Rules are ``(label, predicate)`` pairs evaluated in order; the first
    predicate that returns ``True`` wins.  If no rule matches, *default* is
    used (empty string by default).
    """

    def __init__(
        self,
        source,
        column: str,
        rules: list[tuple[str, Callable[[dict], bool]]],
        default: str = "",
    ) -> None:
        if not column:
            raise ValueError("column name must not be empty")
        if not rules:
            raise ValueError("at least one rule is required")
        self._source = source
        self._column = column
        self._rules = rules
        self._default = default

    # ------------------------------------------------------------------
    def headers(self) -> list[str]:
        return self._source.headers() + [self._column]

    def rows(self) -> Iterable[dict]:
        col = self._column
        rules = self._rules
        default = self._default
        for row in self._source.rows():
            tag = default
            for label, predicate in rules:
                try:
                    if predicate(row):
                        tag = label
                        break
                except Exception:
                    pass
            yield {**row, col: tag}

    def row_count(self) -> int:
        return self._source.row_count()

    def __repr__(self) -> str:  # pragma: no cover
        return f"CSVTagger(column={self._column!r}, rules={len(self._rules)})"
