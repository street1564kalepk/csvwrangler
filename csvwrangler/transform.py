from typing import Callable, Dict, Iterator, List, Optional


class CSVTransform:
    """
    Applies column-level transformations to rows in a CSV pipeline.
    Supports renaming columns, adding computed columns, and applying
    per-cell transformation functions.
    """

    def __init__(self, source):
        self._source = source
        self._renames: Dict[str, str] = {}
        self._computed: Dict[str, Callable[[Dict], str]] = {}
        self._transforms: Dict[str, Callable[[str], str]] = {}

    @property
    def headers(self) -> List[str]:
        base = list(self._source.headers)
        renamed = [self._renames.get(h, h) for h in base]
        return renamed + list(self._computed.keys())

    def rename(self, old_name: str, new_name: str) -> "CSVTransform":
        """Rename a column from old_name to new_name."""
        self._renames[old_name] = new_name
        return self

    def add_column(
        self, name: str, fn: Callable[[Dict], str]
    ) -> "CSVTransform":
        """Add a new computed column using a function that receives each row dict."""
        self._computed[name] = fn
        return self

    def apply(self, column: str, fn: Callable[[str], str]) -> "CSVTransform":
        """Apply a transformation function to every value in a column."""
        self._transforms[column] = fn
        return self

    def rows(self) -> Iterator[Dict[str, str]]:
        """Yield transformed rows as dicts keyed by (possibly renamed) headers."""
        for row in self._source.rows():
            new_row: Dict[str, str] = {}

            for key, value in row.items():
                # Apply value transformation if registered (use original key)
                if key in self._transforms:
                    value = self._transforms[key](value)

                # Apply rename
                new_key = self._renames.get(key, key)
                new_row[new_key] = value

            # Add computed columns
            for col_name, fn in self._computed.items():
                new_row[col_name] = fn(row)

            yield new_row

    def __repr__(self) -> str:
        return (
            f"CSVTransform(renames={self._renames}, "
            f"computed={list(self._computed.keys())}, "
            f"transforms={list(self._transforms.keys())})"
        )
