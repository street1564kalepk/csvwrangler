"""Split rows into groups based on how many non-empty fields each row has."""

from __future__ import annotations
from typing import List, Dict


class CSVFieldCountSplitter:
    """Partition rows by the number of non-empty (truthy) fields they contain."""

    def __init__(self, source) -> None:
        self._source = source
        self._built: bool = False
        self._groups: Dict[int, List[dict]] = {}

    @property
    def headers(self) -> List[str]:
        return self._source.headers

    def _ensure_built(self) -> None:
        if self._built:
            return
        for row in self._source.rows():
            count = sum(1 for v in row.values() if str(v).strip() != "")
            self._groups.setdefault(count, []).append(row)
        self._built = True

    @property
    def group_keys(self) -> List[int]:
        self._ensure_built()
        return sorted(self._groups.keys())

    def group_count(self) -> int:
        self._ensure_built()
        return len(self._groups)

    def rows_for(self, field_count: int) -> List[dict]:
        self._ensure_built()
        return list(self._groups.get(field_count, []))

    def all_groups(self) -> Dict[int, List[dict]]:
        self._ensure_built()
        return {k: list(v) for k, v in self._groups.items()}
