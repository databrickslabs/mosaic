from test.context import api

from .mosaic_test_case import MosaicTestCase

from pyspark.sql.dataframe import DataFrame


class MosaicTestCaseWithGDAL(MosaicTestCase):
    def setUp(self) -> None:
        return super().setUp()

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        api.enable_mosaic(cls.spark)
        api.enable_gdal(cls.spark)

    def generate_singleband_raster_df(self) -> DataFrame:
        return (
            self.spark.read.format("gdal")
            .option("raster.read.strategy", "in_memory")
            .load("test/data/MCD43A4.A2018185.h10v07.006.2018194033728_B04.TIF")
        )
