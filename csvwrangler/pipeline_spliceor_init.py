"""Auto-import shim so that importing csvwrangler activates the splice patch."""
import csvwrangler.pipeline_spliceor_patch  # noqa: F401
