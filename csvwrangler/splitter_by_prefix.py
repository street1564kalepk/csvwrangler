"""CSVPrefixSplitter – split rows into groups based on a column value prefix."""

from __future__ import annotations

from typing import Dict, Iterable, List


class CSVPrefixSplitter:
    """Split a CSV source into groups whose key column value starts with a given prefix.

    Rows whose key column value does not match any prefix are placed in a
    catch-all group keyed by ``"__other__"``.
    """

    def __init__(
        self,
        source,
        column: str,
        prefixes: List[str],
        *,
        case_sensitive: bool = True,
    ) -> None:
        if column not in source.headers:
            raise ValueError(f"Column '{column}' not found in source headers.")
        if not prefixes:
            raise ValueError("At least one prefix must be supplied.")

        self._source = source
        self._column = column
        self._prefixes = prefixes
        self._case_sensitive = case_sensitive
        self._built: bool = False
        self._groups: Dict[str, List[dict]] = {}

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    @property
    def headers(self) -> List[str]:
        return list(self._source.headers)

    def _ensure_built(self) -> None:
        if self._built:
            return
        groups: Dict[str, List[dict]] = {p: [] for p in self._prefixes}
        groups["__other__"] = []
        col = self._column
        for row in self._source.rows:
            val: str = row.get(col, "")
            cmp_val = val if self._case_sensitive else val.lower()
            matched = False
            for prefix in self._prefixes:
                cmp_prefix = prefix if self._case_sensitive else prefix.lower()
                if cmp_val.startswith(cmp_prefix):
                    groups[prefix].append(row)
                    matched = True
                    break
            if not matched:
                groups["__other__"].append(row)
        self._groups = groups
        self._built = True

    @property
    def group_keys(self) -> List[str]:
        self._ensure_built()
        return list(self._groups.keys())

    def group(self, key: str) -> List[dict]:
        self._ensure_built()
        if key not in self._groups:
            raise KeyError(f"No group for prefix '{key}'.")
        return list(self._groups[key])

    def all_groups(self) -> Dict[str, List[dict]]:
        self._ensure_built()
        return {k: list(v) for k, v in self._groups.items()}

    @property
    def group_count(self) -> int:
        self._ensure_built()
        return len(self._groups)
