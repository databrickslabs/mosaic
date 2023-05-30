import random

from pyspark.sql.functions import abs, col, first, lit, sqrt

from .context import api
from .context import readers
from .context import rst
from .utils import MosaicTestCaseWithGDAL

class TestRaster(MosaicTestCaseWithGDAL):

    def test_raster(self):
        self.spark.read\
            .format("gdal")\
            .option("driverName", "GTiff")\
            .load("/root/mosaic/src/test/resources/binary/grid_tiles_tif")\
            .withColumn("grid_tiles", api.rst_gridtiles("path", lit(6)))\
            .show(20, False)