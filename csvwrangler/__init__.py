"""csvwrangler – fast CSV filtering, joining, and transformation."""
from csvwrangler.pipeline import Pipeline  # noqa: F401

# Apply optional pipeline patches
from csvwrangler.pipeline_validate_patch import _validate as _vp; _vp()
from csvwrangler.pipeline_encode_patch import _patch as _ep; _ep()
from csvwrangler.pipeline_scorer_patch import _patch as _sp; _sp()
from csvwrangler.pipeline_shifter_patch import _patch as _shp; _shp()
from csvwrangler.pipeline_grouper_patch import _patch as _gp; _gp()
