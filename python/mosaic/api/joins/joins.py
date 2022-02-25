from mosaic.config import config
from mosaic.core.mosaic_frame import MosaicFrame

from py4j.java_gateway import JavaClass, JavaObject

from pyspark.sql import DataFrame
from pyspark import SparkContext


class PointInPolygonJoin:
    @classmethod
    def join(cls, points: MosaicFrame, polygons: MosaicFrame) -> DataFrame:
        pass
