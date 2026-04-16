"""Monkey-patches Pipeline with .encode() method."""
from csvwrangler.encoder import CSVEncoder


def encode(self, columns: list[str], mode: str = "encode", encoding: str = "base64"):
    """Encode or decode column values using base64 or url encoding.

    Args:
        columns: list of column names to transform.
        mode: 'encode' or 'decode'.
        encoding: 'base64' or 'url'.

    Returns:
        Pipeline: self, for chaining.
    """
    self._source = CSVEncoder(self._source, columns, mode=mode, encoding=encoding)
    return self


def _patch():
    from csvwrangler.pipeline import Pipeline
    if not hasattr(Pipeline, "encode"):
        Pipeline.encode = encode


_patch()
