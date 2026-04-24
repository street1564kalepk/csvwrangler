"""CSVConditionSplitter – split rows into named groups based on boolean conditions.

Each condition is a (label, predicate) pair where predicate is a callable
that receives a row dict and returns a truthy value.  Rows that match no
condition are collected under the special key ``'__unmatched__'`` unless
``drop_unmatched=True`` is passed.

Example usage::

    splitter = CSVConditionSplitter(
        source,
        conditions=[
            ("high", lambda r: float(r["score"]) >= 80),
            ("mid",  lambda r: 50 <= float(r["score"]) < 80),
            ("low",  lambda r: float(r["score"]) < 50),
        ],
    )
    for label, src in splitter.groups().items():
        print(label, list(src.rows()))
"""

from __future__ import annotations

from typing import Callable, Dict, Iterable, List, Tuple

_UNMATCHED = "__unmatched__"


class CSVConditionSplitter:
    """Split a CSV source into named groups using caller-supplied predicates."""

    def __init__(
        self,
        source,
        conditions: List[Tuple[str, Callable[[dict], bool]]],
        *,
        drop_unmatched: bool = False,
        first_match_only: bool = True,
    ) -> None:
        """
        Parameters
        ----------
        source:
            Any object exposing ``headers`` (list[str]) and ``rows()``
            (iterable of dicts).
        conditions:
            Ordered list of ``(label, predicate)`` pairs.  Labels must be
            unique.
        drop_unmatched:
            When *True*, rows that satisfy no condition are silently
            discarded.  When *False* (default) they accumulate under the
            ``'__unmatched__'`` key.
        first_match_only:
            When *True* (default) each row is assigned to the first
            matching group only.  When *False* a row may appear in
            multiple groups.
        """
        if not conditions:
            raise ValueError("At least one condition must be provided.")

        labels = [label for label, _ in conditions]
        if len(labels) != len(set(labels)):
            raise ValueError("Condition labels must be unique.")

        self._source = source
        self._conditions = conditions
        self._drop_unmatched = drop_unmatched
        self._first_match_only = first_match_only
        self._built: Dict[str, List[dict]] | None = None

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    @property
    def headers(self) -> List[str]:
        """Column headers (identical for every group)."""
        return list(self._source.headers)

    def _ensure_built(self) -> None:
        if self._built is not None:
            return

        buckets: Dict[str, List[dict]] = {
            label: [] for label, _ in self._conditions
        }
        if not self._drop_unmatched:
            buckets[_UNMATCHED] = []

        for row in self._source.rows():
            placed = False
            for label, predicate in self._conditions:
                try:
                    matched = bool(predicate(row))
                except Exception:  # noqa: BLE001 – treat errors as no-match
                    matched = False
                if matched:
                    buckets[label].append(row)
                    placed = True
                    if self._first_match_only:
                        break

            if not placed and not self._drop_unmatched:
                buckets[_UNMATCHED].append(row)

        # Remove empty __unmatched__ bucket to keep output tidy
        if not self._drop_unmatched and not buckets[_UNMATCHED]:
            del buckets[_UNMATCHED]

        self._built = buckets

    def group_keys(self) -> List[str]:
        """Return the labels for non-empty groups (in condition order)."""
        self._ensure_built()
        return list(self._built.keys())  # type: ignore[union-attr]

    def group_count(self) -> int:
        """Number of non-empty groups."""
        return len(self.group_keys())

    def rows_for(self, label: str) -> List[dict]:
        """Return the list of rows assigned to *label*."""
        self._ensure_built()
        if label not in self._built:  # type: ignore[operator]
            raise KeyError(f"Group {label!r} not found.")
        return list(self._built[label])  # type: ignore[index]

    def groups(self) -> Dict[str, "_GroupSource"]:
        """Return a mapping of label → source-like object for each group."""
        self._ensure_built()
        return {
            label: _GroupSource(self.headers, rows)
            for label, rows in self._built.items()  # type: ignore[union-attr]
        }


class _GroupSource:
    """Lightweight source wrapper returned by :meth:`CSVConditionSplitter.groups`."""

    def __init__(self, headers: List[str], rows: List[dict]) -> None:
        self._headers = headers
        self._rows = rows

    @property
    def headers(self) -> List[str]:
        return list(self._headers)

    def rows(self) -> Iterable[dict]:
        return iter(self._rows)

    @property
    def row_count(self) -> int:
        return len(self._rows)
