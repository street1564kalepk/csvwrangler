"""CSVJoiner: join two CSV sources on a common key column."""

from typing import Iterator


class CSVJoiner:
    """Performs an inner or left join between two CSV sources on a key column."""

    SUPPORTED_JOINS = ("inner", "left")

    def __init__(self, left, right, on: str, how: str = "inner"):
        """
        :param left:  A source with .headers and .rows() (e.g. CSVReader / Pipeline).
        :param right: A source with .headers and .rows().
        :param on:    Column name present in both sources to join on.
        :param how:   'inner' (default) or 'left'.
        """
        if how not in self.SUPPORTED_JOINS:
            raise ValueError(
                f"Unsupported join type '{how}'. Choose from {self.SUPPORTED_JOINS}."
            )
        if on not in left.headers:
            raise KeyError(f"Join key '{on}' not found in left source headers.")
        if on not in right.headers:
            raise KeyError(f"Join key '{on}' not found in right source headers.")

        self._left = left
        self._right = right
        self._on = on
        self._how = how

        # Build merged header list: left headers + right headers (excluding key)
        right_extra = [h for h in right.headers if h != on]
        self._headers = list(left.headers) + right_extra

    @property
    def headers(self) -> list:
        return self._headers

    def _build_right_index(self) -> dict:
        """Load the right source into memory, indexed by the join key."""
        index: dict = {}
        for row in self._right.rows():
            key = row[self._on]
            index.setdefault(key, []).append(row)
        return index

    def rows(self) -> Iterator[dict]:
        right_index = self._build_right_index()
        right_extra_cols = [h for h in self._right.headers if h != self._on]
        empty_right = {col: "" for col in right_extra_cols}

        for left_row in self._left.rows():
            key = left_row[self._on]
            matched = right_index.get(key)

            if matched:
                for right_row in matched:
                    merged = dict(left_row)
                    for col in right_extra_cols:
                        merged[col] = right_row.get(col, "")
                    yield merged
            elif self._how == "left":
                merged = dict(left_row)
                merged.update(empty_right)
                yield merged
            # inner join: skip rows with no match

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"CSVJoiner(on={self._on!r}, how={self._how!r}, "
            f"headers={self._headers})"
        )
