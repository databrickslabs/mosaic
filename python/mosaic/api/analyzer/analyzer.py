from mosaic.config import config

from py4j.java_gateway import JavaClass, JavaObject

from pyspark.sql import DataFrame
from pyspark import SparkContext


class MosaicAnalyzer:
    sc: SparkContext
    _analyzerClass: JavaClass
    _analyzer: JavaObject

    def __init__(self):
        self.sc = config.mosaic_spark.sparkContext
        self._analyzerClass = getattr(
            self.sc._jvm.com.databricks.mosaic.sql, "MosaicAnalyzer$"
        )
        self._analyzer = getattr(self._analyzerClass, "MODULE$")

    def get_optimal_resolution(self, df: DataFrame, geometry_column_name: str) -> int:
        return self._analyzer.getOptimalResolution(df._jdf, geometry_column_name)

    def get_sample_fraction(self) -> int:
        return self._analyzer.getSampleFraction()

    def set_sample_fraction(self, sample_fraction: float):
        return self._analyzer.setSampleFraction(sample_fraction)
