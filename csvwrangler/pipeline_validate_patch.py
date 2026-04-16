"""
Patch helper – monkey-patches Pipeline.validate() at import time.
Import this module before using Pipeline.validate().

In production, merge validate() directly into pipeline.py.
"""
from csvwrangler.pipeline import Pipeline
from csvwrangler.validator import CSVValidator


def _validate(self, rules: dict, mode: str = "drop", tag_column: str = "_errors"):
    """Validate rows against column rules.

    Parameters
    ----------
    rules      : {column: callable} – callable returns True when the value is valid.
    mode       : 'drop' – remove invalid rows (default)
                 'tag'  – keep all rows, add an error column
                 'raise'– raise ValueError on first invalid row
    tag_column : name of the error tag column (used when mode='tag').
    """
    return self._chain(CSVValidator(self._source, rules, mode=mode, tag_column=tag_column))


if not hasattr(Pipeline, "validate"):
    Pipeline.validate = _validate
