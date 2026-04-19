"""CSVReplacer – find-and-replace values in specified columns."""
from __future__ import annotations
from typing import Dict, Any, Iterator


class CSVReplacer:
    """Replace exact values (or substrings) in one or more columns."""

    def __init__(
        self,
        source,
        column_replacements: Dict[str, Dict[str, str]],
        *,
        substring: bool = False,
    ):
        """
        Parameters
        ----------
        source:
            Any object exposing `.headers` and `.rows`.
        column_replacements:
            Mapping of column -> {old_value: new_value, ...}.
        substring:
            If True, perform substring replacement instead of exact match.
        """
        self._source = source
        self._replacements = column_replacements
        self._substring = substring
        unknown = set(column_replacements) - set(source.headers)
        if unknown:
            raise ValueError(f"Unknown columns: {sorted(unknown)}")

    # ------------------------------------------------------------------
    @property
    def headers(self):
        return self._source.headers

    def rows(self) -> Iterator[Dict[str, Any]]:
        for row in self._source.rows():
            new_row = dict(row)
            for col, mapping in self._replacements.items():
                val = new_row.get(col, "")
                if self._substring:
                    for old, new in mapping.items():
                        val = val.replace(old, new)
                    new_row[col] = val
                else:
                    if val in mapping:
                        new_row[col] = mapping[val]
            yield new_row

    @property
    def row_count(self) -> int:
        return sum(1 for _ in self.rows())
