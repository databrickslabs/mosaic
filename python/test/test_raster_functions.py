import random

from pyspark.sql.functions import abs, col, first, lit, sqrt, array

from .context import api
from .utils import MosaicTestCaseWithGDAL


class TestRasterFunctions(MosaicTestCaseWithGDAL):
    def test_read_raster(self):
        result = self.generate_singleband_raster_df().first()
        self.assertEqual(result.length, 1067862)
        self.assertEqual(result.x_size, 2400)
        self.assertEqual(result.y_size, 2400)
        self.assertEqual(result.srid, 0)
        self.assertEqual(result.bandCount, 1)
        self.assertEqual(
            result.metadata["LONGNAME"],
            "MODIS/Terra+Aqua BRDF/Albedo Nadir BRDF-Adjusted Ref Daily L3 Global - 500m",
        )
        self.assertEqual(result.tile["driver"], "GTiff")

    def test_raster_scalar_functions(self):
        __all__ = [
            "rst_bandmetadata",
            "rst_boundingbox",
            "rst_clip",
            "rst_combineavg",
            "rst_frombands",
            "rst_fromfile",
            "rst_georeference",
            "rst_getnodata",
            "rst_getsubdataset",
            "rst_height",
            "rst_initnodata",
            "rst_isempty",
            "rst_memsize",
            "rst_merge",
            "rst_metadata",
            "rst_ndvi",
            "rst_numbands",
            "rst_pixelheight",
            "rst_pixelwidth",
            "rst_rastertogridavg",
            "rst_rastertogridcount",
            "rst_rastertogridmax",
            "rst_rastertogridmedian",
            "rst_rastertogridmin",
            "rst_rastertoworldcoordx",
            "rst_rastertoworldcoordy",
            "rst_rastertoworldcoord",
            "rst_retile",
            "rst_rotation",
            "rst_scalex",
            "rst_scaley",
            "rst_setnodata",
            "rst_skewx",
            "rst_skewy",
            "rst_srid",
            "rst_subdatasets",
            "rst_subdivide",
            "rst_summary",
            "rst_tessellate",
            "rst_to_overlapping_tiles",
            "rst_tryopen",
            "rst_upperleftx",
            "rst_upperlefty",
            "rst_width",
            "rst_worldtorastercoordx",
            "rst_worldtorastercoordy",
            "rst_worldtorastercoord",
        ]

        result = (
            self.generate_singleband_raster_df()
            .withColumn("rst_bandmetadata", api.rst_bandmetadata("tile", lit(1)))
            .withColumn("rst_boundingbox", api.rst_boundingbox("tile"))
            .withColumn(
                "rst_boundingbox", api.st_buffer("rst_boundingbox", lit(-0.001))
            )
            .withColumn("rst_clip", api.rst_clip("tile", "rst_boundingbox"))
            .withColumn(
                "rst_combineavg",
                api.rst_combineavg(array(col("tile"), col("rst_clip"))),
            )
            .withColumn("rst_frombands", api.rst_frombands(array("tile", "tile")))
            .withColumn("tile_from_file", api.rst_fromfile("path", lit(-1)))
            .withColumn("rst_georeference", api.rst_georeference("tile"))
            .withColumn("rst_getnodata", api.rst_getnodata("tile"))
            .withColumn("rst_subdatasets", api.rst_subdatasets("tile"))
            # .withColumn("rst_getsubdataset", api.rst_getsubdataset("tile"))
            .withColumn("rst_height", api.rst_height("tile"))
            .withColumn("rst_initnodata", api.rst_initnodata("tile"))
            .withColumn("rst_isempty", api.rst_isempty("tile"))
            .withColumn("rst_memsize", api.rst_memsize("tile"))
            .withColumn("rst_merge", api.rst_merge(array("tile", "tile")))
            .withColumn("rst_metadata", api.rst_metadata("tile"))
            .withColumn("rst_ndvi", api.rst_ndvi("tile", lit(1), lit(1)))
        )

        result.write.format("noop").mode("overwrite").save()
        # result.select("rst_subdatasets").show(truncate=False)
