from typing import Any
from py4j.java_gateway import JavaClass, JavaObject, JavaPackage, JavaMember
from pyspark import SparkContext

from mosaic.config import config

from mosaic.utils.types import ColumnOrName, scala_option

from pyspark.sql import DataFrame
from pyspark.sql.column import _to_java_column


class MosaicFrame(DataFrame):
    _mosaicFrameClass: JavaClass
    _mosaicFrameObject: JavaObject
    _mosaicFrame: JavaObject
    _df: JavaObject
    _geometry_column_name: str

    def __init__(
        self,
            df: DataFrame,
            geometry_column_name: str
    ):
        super(MosaicFrame, self).__init__(df._jdf, config.sql_context)
        self._df = df._jdf
        self._geometry_column_name = geometry_column_name
        self.sc = config.mosaic_spark.sparkContext
        self._mosaicFrameClass = getattr(
            self.sc._jvm.com.databricks.mosaic.sql, "MosaicFrame$"
        )
        self._mosaicFrameObject = getattr(self._mosaicFrameClass, "MODULE$")
        self._mosaicFrame = self._mosaicFrameObject.apply(
            self._df,
            self._geometry_column_name
        )

    def get_optimal_resolution(self, sample_rows: int = 1000) -> int:
        return self._mosaicFrame.getOptimalResolution(sample_rows)

    def set_index_resolution(self, resolution: int) -> "MosaicFrame":
        self._mosaicFrame = self._mosaicFrame.setIndexResolution(resolution)
        return self

    def apply_index(self) -> "MosaicFrame":
        self._mosaicFrame = self._mosaicFrame.applyIndex(True)
        return self

    def join(self, other: "MosaicFrame") -> "MosaicFrame":
        self._mosaicFrame = self._mosaicFrame.join(other._mosaicFrame)
        return self

#
#     def
#
#     def get_resolution_metrics(
#         self, lower_limit: int = 10, upper_limit: int = 100, fraction_sample: float = 0.01
#     ) -> DataFrame:
#         return self._mosaicFrame.getResolutionMetrics(lower_limit, upper_limit)
#
#     def join(self, other: 'MosaicFrame', join_type: MosaicJoinType):
#         pass
#
#
# def setGeometryColumn:
#     pass
#
# def getPointIndexColumn:
#     pass
#
# def getPointIndexColumnName:
#     pass
#
# def getFillIndexColumn:
#     pass
#
# def getFillIndexColumnName:
#     pass
#
# def getChipColumn:
#     pass
#
# def getChipColumnName:
#     pass
#
# def getGeometryId:
#     pass
#
# def getChipFlagColumn:
#     pass
#
# def getChipFlagColumnName:
#     pass
#
# def join:
#     pass
#
# def isIndexed:
#     pass
#
# def getGeometryType:
#     pass
#
# def setIndexResolution:
#     pass
#
# getGeometryColumn
# withColumn
# getOptimalResolution

# getOptimalResolution
# analyzer
# withPrefix
# select
# flattenGeometries
# getIndexResolution
# applyIndex
#     def count(self):
#         self._mosaicFrame.count()
