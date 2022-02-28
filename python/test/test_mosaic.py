from pyspark.sql.functions import _to_java_column, col

from .context import MosaicContext, MosaicLibraryHandler
from .utils import SparkTestCase


class TestMosaicContext(SparkTestCase):
    def test_invoke_function(self):
        _ = MosaicLibraryHandler(self.spark)
        context = MosaicContext(self.spark)

        expected = [
            "POINT (0 0)",
            "POINT (1 1)",
            "POINT (2 2)",
            "POINT (3 3)",
            "POINT (4 4)",
        ]

        df = (
            self.spark.range(5)
            .withColumn("lat", col("id").cast("double"))
            .withColumn("lon", col("id").cast("double"))
        )
        operator_1 = context.invoke_function(
            "st_point", _to_java_column("lon"), _to_java_column("lat")
        )
        operator_2 = context.invoke_function("st_astext", _to_java_column("points"))
        result = (
            df.withColumn("points", operator_1)
            .withColumn("points", operator_2)
            .collect()
        )
        self.assertListEqual([rw.points for rw in result], expected)
