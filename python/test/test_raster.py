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
            .option("driverName", "NetCDF")\
            .load("/root/mosaic/src/test/resources/binary/netcdf-coral")\
            .withColumn("grid_tiles", api.rst_gridtiles(lit("""NETCDF:"///root/mosaic/src/test/resources/binary/netcdf-coral/ct5km_baa-max-7d_v3.1_20220103.nc":bleaching_alert_area"""), lit(7)))\
            .show(20, False)