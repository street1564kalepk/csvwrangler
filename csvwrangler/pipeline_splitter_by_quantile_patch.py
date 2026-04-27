"""Patch Pipeline with a .split_by_quantile() method."""
from __future__ import annotations

from typing import Dict

from csvwrangler.splitter_by_quantile import CSVQuantileSplitter


def split_by_quantile(self, column: str, n_quantiles: int = 4) -> Dict[str, object]:
    """Split the pipeline into *n_quantiles* quantile buckets by *column*.

    Returns a ``dict`` mapping bucket labels (``"Q1"`` … ``"Qn"``) to new
    ``Pipeline`` objects so each group can be processed independently.
    """
    from csvwrangler.pipeline import Pipeline  # local import to avoid circulars

    splitter = CSVQuantileSplitter(self._source, column, n_quantiles=n_quantiles)

    class _GroupSrc:
        def __init__(self, sp: CSVQuantileSplitter, key: str) -> None:
            self._sp = sp
            self._key = key

        @property
        def headers(self):
            return self._sp.headers

        def rows(self):
            yield from self._sp.rows(self._key)

    result: Dict[str, Pipeline] = {}
    for key in splitter.group_keys:
        p = Pipeline.__new__(Pipeline)
        p._source = _GroupSrc(splitter, key)  # type: ignore[attr-defined]
        result[key] = p
    return result


def _patch(pipeline_cls) -> None:
    pipeline_cls.split_by_quantile = split_by_quantile
