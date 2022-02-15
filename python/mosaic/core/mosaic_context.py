from typing import Any

from py4j.protocol import Py4JJavaError
from pyspark.sql import SparkSession
from pyspark.sql.column import Column as MosaicColumn


class MosaicContext:

    _context = None

    def __init__(self, spark: SparkSession):
        sc = spark.sparkContext
        MosaicContextClass = getattr(
            sc._jvm.com.databricks.mosaic.functions, "MosaicContext"
        )
        mosaicPackageRef = getattr(sc._jvm.com.databricks.mosaic, "package$")
        mosaicPackageObject = getattr(mosaicPackageRef, "MODULE$")

        try:
            geometry_api = spark.conf.get("spark.databricks.mosaic.geometry.api")
        except Py4JJavaError as e:
            geometry_api = "OGC"

        try:
            index_system = spark.conf.get("spark.databricks.mosaic.index.system")
        except Py4JJavaError as e:
            index_system = "H3"

        IndexSystemClass = getattr(mosaicPackageObject, index_system)
        GeometryAPIClass = getattr(mosaicPackageObject, geometry_api)

        self._context = MosaicContextClass.build(IndexSystemClass(), GeometryAPIClass())

    def invoke_function(self, name: str, *args: Any) -> MosaicColumn:
        func = getattr(self._context.functions(), name)
        return MosaicColumn(func(*args))
