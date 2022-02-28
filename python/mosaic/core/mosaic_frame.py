from enum import Enum
from typing import Any
from py4j.java_gateway import JavaClass, JavaObject, JavaPackage, JavaMember
from pyspark import SparkContext

from mosaic.config import config

# from mosaic import MosaicAnalyzer
from mosaic.utils.types import ColumnOrName, scala_option

from pyspark.sql import DataFrame
from pyspark.sql.column import _to_java_column


class MosaicJoinType:
    sc: SparkContext
    _joinTypeEnumObject: JavaClass
    _POINT_IN_POLYGON: JavaObject
    _POLYGON_INTERSECTION: JavaObject

    def __init__(self):
        self.sc = config.mosaic_spark.sparkContext
        self._joinTypeEnumObject = self.sc._jvm.com.databricks.mosaic.sql.MosaicJoinType
        self._POINT_IN_POLYGON = self._joinTypeEnumObject.POINT_IN_POLYGON()
        self._POLYGON_INTERSECTION = self._joinTypeEnumObject.POLYGON_INTERSECTION()

    @property
    def point_in_polygon(self):
        return self._POINT_IN_POLYGON

    @property
    def polygon_intersection(self):
        return self._POLYGON_INTERSECTION

    def from_id(self, value: int) -> JavaObject:
        return self._joinTypeEnumObject.fromId(value)

    def from_string(self, value) -> JavaObject:
        return self._joinTypeEnumObject.fromString(value)


class MosaicFrame:
    _mosaicFrameClass: JavaClass
    _mosaicFrameObject: JavaObject
    _mosaicFrame: JavaObject
    _df: JavaObject
    _chipColumn: JavaObject
    _chipFlagColumn: JavaObject
    _indexColumn: JavaObject
    _geometryColumn: JavaObject

    def __init__(
        self,
        df: DataFrame,
        geometry_column: ColumnOrName,
        chip_column: ColumnOrName = "",
        chip_flag_column: ColumnOrName = "",
        index_column: ColumnOrName = ""
    ):
        self._df = df._jdf
        self._chipColumn = _to_java_column(chip_column)
        self._chipFlagColumn = _to_java_column(chip_flag_column)
        self._indexColumn = _to_java_column(index_column)
        self._geometryColumn = _to_java_column(geometry_column)
        self.sc = config.mosaic_spark.sparkContext
        self._mosaicFrameClass = getattr(
            self.sc._jvm.com.databricks.mosaic.sql, "MosaicFrame$"
        )
        self._mosaicFrameObject = getattr(self._mosaicFrameClass, "MODULE$")
        self._mosaicFrame = self._mosaicFrameObject.apply(
            self._df,
            scala_option(self._chipColumn),
            scala_option(self._chipFlagColumn),
            scala_option(self._indexColumn),
            scala_option(self._geometryColumn),
        )

    def get_resolution_metrics(
        self, lower_limit: int = 10, upper_limit: int = 100, fraction_sample: float = 0.01
    ) -> DataFrame:
        return self._mosaicFrame.getResolutionMetrics(lower_limit, upper_limit)

    def join(self, other: 'MosaicFrame', join_type: MosaicJoinType):
        pass


