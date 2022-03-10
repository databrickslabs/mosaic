from typing import Optional
from py4j.java_gateway import JavaClass, JavaObject
from pyspark.sql import DataFrame

from mosaic.config import config


class MosaicFrame(DataFrame):
    _mosaicFrameClass: JavaClass
    _mosaicFrameObject: JavaObject
    _mosaicFrame: JavaObject
    _df: JavaObject
    _geometry_column_name: str

    def __init__(self, df: DataFrame, geometry_column_name: str):
        super(MosaicFrame, self).__init__(df._jdf, config.sql_context)
        self._df = df._jdf
        self._geometry_column_name = geometry_column_name
        self.sc = config.mosaic_spark.sparkContext
        self._mosaicFrameClass = getattr(
            self.sc._jvm.com.databricks.mosaic.sql, "MosaicFrame$"
        )
        self._mosaicFrameObject = getattr(self._mosaicFrameClass, "MODULE$")
        self._mosaicFrame = self._mosaicFrameObject.apply(
            self._df, self._geometry_column_name
        )

    def get_optimal_resolution(
        self, sample_rows: Optional[int] = None, sample_fraction: Optional[float] = None
    ) -> int:
        if sample_rows:
            return self._mosaicFrame.getOptimalResolution(sample_rows)
        if sample_fraction:
            return self._mosaicFrame.getOptimalResolution(sample_fraction)
        return self._mosaicFrame.getOptimalResolution()

    def set_index_resolution(self, resolution: int) -> "MosaicFrame":
        self._mosaicFrame = self._mosaicFrame.setIndexResolution(resolution)
        return self

    def apply_index(self) -> "MosaicFrame":
        self._mosaicFrame = self._mosaicFrame.applyIndex(True, True)
        return self

    def join(self, other: "MosaicFrame") -> "MosaicFrame":
        self._mosaicFrame = self._mosaicFrame.join(other._mosaicFrame)
        return self

    @property
    def geometry_column(self):
        return self._mosaicFrame.getFocalGeometryColumnName()

    def set_geometry_column(self, column_name: str) -> "MosaicFrame":
        self._mosaicFrame = self._mosaicFrame.setGeometryColumn(column_name)
        return self

    def _prettified(self) -> DataFrame:
        return self._mosaicFrame.prettified
