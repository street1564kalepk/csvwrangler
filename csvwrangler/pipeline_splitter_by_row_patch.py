"""Patch Pipeline with a .split_by_rows(n) method."""
from __future__ import annotations
from csvwrangler.splitter_by_row import CSVRowSplitter


def split_by_rows(self, chunk_size: int) -> "CSVRowSplitter":
    """Return a CSVRowSplitter wrapping the current pipeline source.

    Unlike most pipeline methods this returns a *CSVRowSplitter* directly
    so callers can iterate ``.chunks()`` or address individual chunks via
    ``.chunk(index)``.
    """
    return CSVRowSplitter(self._source, chunk_size)


def _patch(pipeline_cls):
    pipeline_cls.split_by_rows = split_by_rows
