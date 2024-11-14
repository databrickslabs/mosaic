import os
import shutil
from test.context import api

from pyspark.sql.dataframe import DataFrame

from .mosaic_test_case import MosaicTestCase


class MosaicTestCaseWithGDAL(MosaicTestCase):
    check_dir = None
    new_check_dir = None

    def setUp(self) -> None:
        return super().setUp()

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()

        pwd_dir = os.getcwd()
        cls.check_dir = f"{pwd_dir}/checkpoint"
        cls.new_check_dir = f"{pwd_dir}/checkpoint-new"
        if not os.path.exists(cls.check_dir):
            os.makedirs(cls.check_dir)
        if not os.path.exists(cls.new_check_dir):
            os.makedirs(cls.new_check_dir)
        cls.spark.conf.set(
            "spark.databricks.labs.mosaic.raster.checkpoint", cls.check_dir
        )

        api.enable_mosaic(cls.spark)
        api.enable_gdal(cls.spark)

    @classmethod
    def tearDownClass(cls) -> None:
        super().tearDownClass()
        if cls.check_dir is not None and os.path.exists(cls.check_dir):
            shutil.rmtree(cls.check_dir)
        if cls.new_check_dir is not None and os.path.exists(cls.new_check_dir):
            shutil.rmtree(cls.new_check_dir)

    def generate_singleband_raster_df(self) -> DataFrame:
        return (
            self.spark.read.format("gdal")
            .option("raster.read.strategy", "in_memory")
            .load("test/data/MCD43A4.A2018185.h10v07.006.2018194033728_B04.TIF")
        )
