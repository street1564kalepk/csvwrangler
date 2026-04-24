"""Auto-apply the clamper patch when this module is imported."""

from csvwrangler.pipeline import Pipeline
from csvwrangler.pipeline_clamper_patch import _patch

_patch(Pipeline)
