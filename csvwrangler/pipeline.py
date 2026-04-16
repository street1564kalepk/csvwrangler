# This file is intentionally left as a patch note.
# The actual pipeline.py already exists; the validate() method must be added.
# Shown here as the delta/addition to the existing pipeline.py.
#
# Add to Pipeline class:
#
#     def validate(self, rules: dict, mode: str = "drop", tag_column: str = "_errors"):
#         """Validate rows against column rules."""
#         from csvwrangler.validator import CSVValidator
#         return self._chain(CSVValidator(self._source, rules, mode=mode, tag_column=tag_column))
#
# NOTE: This file is a placeholder to document the patch.
# The real pipeline.py is updated separately.
pass
