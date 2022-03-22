from typing import Any

from py4j.java_gateway import JavaClass, JavaObject
from py4j.protocol import Py4JJavaError
from pyspark.sql import SparkSession
from pyspark.sql.column import Column as MosaicColumn


class MosaicContext:

    _context = None
    _geometry_api: str
    _index_system: str
    _mosaicContextClass: JavaClass
    _mosaicPackageRef: JavaClass
    _mosaicPackageObject: JavaObject

    def __init__(self, spark: SparkSession):
        sc = spark.sparkContext
        self._mosaicContextClass = getattr(
            sc._jvm.com.databricks.labs.mosaic.functions, "MosaicContext"
        )
        self._mosaicPackageRef = getattr(sc._jvm.com.databricks.labs.mosaic, "package$")
        self._mosaicPackageObject = getattr(self._mosaicPackageRef, "MODULE$")

        try:
            self._geometry_api = spark.conf.get(
                "spark.databricks.labs.mosaic.geometry.api"
            )
        except Py4JJavaError as e:
            self._geometry_api = "ESRI"

        try:
            self._index_system = spark.conf.get(
                "spark.databricks.labs.mosaic.index.system"
            )
        except Py4JJavaError as e:
            self._index_system = "H3"

        IndexSystemClass = getattr(self._mosaicPackageObject, self._index_system)
        GeometryAPIClass = getattr(self._mosaicPackageObject, self._geometry_api)

        self._context = self._mosaicContextClass.build(
            IndexSystemClass(), GeometryAPIClass()
        )

    def invoke_function(self, name: str, *args: Any) -> MosaicColumn:
        func = getattr(self._context.functions(), name)
        return MosaicColumn(func(*args))

    @property
    def geometry_api(self):
        return self._geometry_api

    @property
    def index_system(self):
        return self._index_system
