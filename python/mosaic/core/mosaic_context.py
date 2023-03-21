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
    _mosaicGDALObject: JavaObject

    def __init__(self, spark: SparkSession):
        sc = spark.sparkContext
        self._mosaicContextClass = getattr(
            sc._jvm.com.databricks.labs.mosaic.functions, "MosaicContext"
        )
        self._mosaicPackageRef = getattr(sc._jvm.com.databricks.labs.mosaic, "package$")
        self._mosaicPackageObject = getattr(self._mosaicPackageRef, "MODULE$")
        self._mosaicGDALObject = getattr(sc._jvm.com.databricks.labs.mosaic.gdal, "MosaicGDAL")
        self._indexSystemFactory = getattr(sc._jvm.com.databricks.labs.mosaic.core.index, "IndexSystemFactory")

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

        try:
            self._raster_api = spark.conf.get(
                "spark.databricks.labs.mosaic.raster.api"
            )
        except Py4JJavaError as e:
            self._raster_api = "GDAL"

        IndexSystem = self._indexSystemFactory.getIndexSystem(self._index_system)
        GeometryAPIClass = getattr(self._mosaicPackageObject, self._geometry_api)
        RasterAPIClass   = getattr(self._mosaicPackageObject, self._raster_api)

        self._context = self._mosaicContextClass.build(
            IndexSystem, GeometryAPIClass(), RasterAPIClass()
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

    def enable_gdal(self, spark: SparkSession):
        return self._mosaicGDALObject.enableGDAL(spark._jsparkSession)
