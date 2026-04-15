"""CSVRenamer — bulk-rename columns via a mapping dictionary."""
from __future__ import annotations
from typing import Dict, Iterable, Iterator


class CSVRenamer:
    """Rename one or more columns in a CSV stream.

    Parameters
    ----------
    source:
        Any object that exposes ``.headers`` (list[str]) and
        ``.rows`` (iterable of dict).
    mapping:
        ``{old_name: new_name}`` pairs.  Unknown keys are silently ignored.
    """

    def __init__(self, source, mapping: Dict[str, str]) -> None:
        if not isinstance(mapping, dict):
            raise TypeError("mapping must be a dict")
        unknown = set(mapping) - set(source.headers)
        if unknown:
            raise ValueError(
                f"Column(s) not found in source: {sorted(unknown)}"
            )
        self._source = source
        self._mapping = mapping
        self._headers: list[str] = [
            mapping.get(h, h) for h in source.headers
        ]

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    @property
    def headers(self) -> list[str]:
        """Return the (possibly renamed) column headers."""
        return list(self._headers)

    @property
    def rows(self) -> Iterator[dict]:
        """Yield rows with keys updated according to the mapping."""
        old_headers = self._source.headers
        new_headers = self._headers
        for row in self._source.rows:
            yield {
                new_headers[i]: row[old_headers[i]]
                for i in range(len(old_headers))
            }

    @property
    def row_count(self) -> int:
        """Return the number of rows (delegates to source when available)."""
        if hasattr(self._source, "row_count"):
            return self._source.row_count
        return sum(1 for _ in self._source.rows)

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"CSVRenamer(mapping={self._mapping!r}, "
            f"headers={self._headers!r})"
        )
