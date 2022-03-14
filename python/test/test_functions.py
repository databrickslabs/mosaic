from pyspark.sql.functions import col

from .context import api
from .utils import MosaicTestCase


class TestFunctions(MosaicTestCase):
    def test_st_point(self):
        expected = [
            "POINT (0 0)",
            "POINT (1 1)",
            "POINT (2 2)",
            "POINT (3 3)",
            "POINT (4 4)",
        ]
        result = (
            self.spark.range(5)
            .select(col("id").cast("double"))
            .withColumn("points", api.st_point("id", "id"))
            .withColumn("points", api.st_astext("points"))
            .collect()
        )
        self.assertListEqual([rw.points for rw in result], expected)
