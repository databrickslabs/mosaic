from pyspark.sql.functions import _to_java_column as pyspark_to_java_column

from .attach import sc
from .mosaic import H3, OGC, _mosaic_invoke_function


MosaicPatchClass = getattr(sc._jvm.com.databricks.mosaic.patch, "MosaicPatch")
mosaicPatch = MosaicPatchClass.apply(H3(), OGC())

#################################
# Patched functions definitions #
# These have to happen after    #
# original bindings             #
#################################


def flatten_polygons(inGeom: "ColumnOrName"):
    return _mosaic_invoke_function(
        "flatten_polygons", mosaicPatch, pyspark_to_java_column(inGeom)
    )


def mosaic_explode(inGeom: "ColumnOrName", resolution: "ColumnOrName"):
    return _mosaic_invoke_function(
        "mosaic_explode",
        mosaicPatch,
        pyspark_to_java_column(inGeom),
        pyspark_to_java_column(resolution),
    )
