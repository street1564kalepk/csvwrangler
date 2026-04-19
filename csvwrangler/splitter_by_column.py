"""Split a CSV source into multiple sources based on selected columns."""
from typing import List, Dict, Iterator


class CSVColumnSplitter:
    """Splits a source into sub-sources, each containing a subset of columns.

    Parameters
    ----------
    source:
        Any object exposing ``.headers`` (list[str]) and ``.rows()``
        (iterator of dicts).
    groups:
        A list of column-name lists.  Each inner list defines one output
        group.  A column may appear in more than one group.
    """

    def __init__(self, source, groups: List[List[str]]) -> None:
        self._source = source
        self._groups = groups
        self._validate()
        self._built: Dict[int, List[dict]] | None = None

    def _validate(self) -> None:
        src_headers = set(self._source.headers)
        for idx, grp in enumerate(self._groups):
            if not grp:
                raise ValueError(f"Group {idx} is empty.")
            missing = [c for c in grp if c not in src_headers]
            if missing:
                raise ValueError(
                    f"Group {idx} references unknown columns: {missing}"
                )

    def _ensure_built(self) -> None:
        if self._built is not None:
            return
        self._built = {i: [] for i in range(len(self._groups))}
        for row in self._source.rows():
            for i, grp in enumerate(self._groups):
                self._built[i].append({col: row.get(col, "") for col in grp})

    @property
    def group_count(self) -> int:
        return len(self._groups)

    def headers_for(self, index: int) -> List[str]:
        return list(self._groups[index])

    def rows_for(self, index: int) -> Iterator[dict]:
        self._ensure_built()
        yield from self._built[index]

    def row_count_for(self, index: int) -> int:
        self._ensure_built()
        return len(self._built[index])
