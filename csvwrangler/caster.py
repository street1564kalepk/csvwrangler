"""CSVCaster: cast column values to a target type (int, float, str, bool)."""

from __future__ import annotations

from typing import Callable, Dict, Iterable, Iterator, List

_CASTERS: Dict[str, Callable[[str], object]] = {
    "int": int,
    "float": float,
    "str": str,
    "bool": lambda v: v.strip().lower() not in ("", "0", "false", "no"),
}


class CSVCaster:
    """Wraps a source and casts specified columns to the requested types.

    Parameters
    ----------
    source:
        Any object exposing ``.headers`` (list[str]) and ``.rows()``
        (iterable of dict).
    casts:
        Mapping of column name -> type name (``'int'``, ``'float'``,
        ``'str'``, or ``'bool'``).
    errors:
        ``'raise'`` (default) – propagate conversion errors;
        ``'ignore'`` – leave the original string value on failure;
        ``'null'`` – replace failed conversions with ``None``.
    """

    def __init__(
        self,
        source,
        casts: Dict[str, str],
        errors: str = "raise",
    ) -> None:
        unknown_types = set(casts.values()) - set(_CASTERS)
        if unknown_types:
            raise ValueError(f"Unknown cast type(s): {unknown_types}")
        if errors not in ("raise", "ignore", "null"):
            raise ValueError(f"errors must be 'raise', 'ignore', or 'null'; got {errors!r}")
        self._source = source
        self._casts = casts
        self._errors = errors

    @property
    def headers(self) -> List[str]:
        return self._source.headers

    def rows(self) -> Iterator[dict]:
        for row in self._source.rows():
            yield self._cast_row(row)

    def _cast_row(self, row: dict) -> dict:
        out = dict(row)
        for col, type_name in self._casts.items():
            if col not in out:
                continue
            fn = _CASTERS[type_name]
            try:
                out[col] = fn(out[col])
            except (ValueError, TypeError):
                if self._errors == "raise":
                    raise
                elif self._errors == "null":
                    out[col] = None
                # else "ignore" – keep original string
        return out

    def row_count(self) -> int:
        return sum(1 for _ in self.rows())

    def __repr__(self) -> str:  # pragma: no cover
        return f"CSVCaster(casts={self._casts!r}, errors={self._errors!r})"
