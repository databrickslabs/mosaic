from test.context import api

from pyspark.sql import DataFrame

from .spark_test_case import SparkTestCase


class MosaicTestCase(SparkTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        api.enable_mosaic(cls.spark)

    def generate_input(self) -> DataFrame:
        return self.spark.createDataFrame(
            [{"wkt": "LINESTRING (30 10, 40 40, 20 40, 10 20, 30 10)"}]
        )
