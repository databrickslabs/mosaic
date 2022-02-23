from typing import Any

from py4j.protocol import Py4JJavaError
from pyspark.sql import SparkSession
from pyspark.sql.column import Column as MosaicColumn


class MosaicContext:

    _context = None
    _geometry_api: str
    _index_system: str

    def __init__(self, spark: SparkSession):
        sc = spark.sparkContext
        MosaicContextClass = getattr(
            sc._jvm.com.databricks.mosaic.functions, "MosaicContext"
        )
        mosaicPackageRef = getattr(sc._jvm.com.databricks.mosaic, "package$")
        mosaicPackageObject = getattr(mosaicPackageRef, "MODULE$")

        try:
            self._geometry_api = spark.conf.get("spark.databricks.mosaic.geometry.api")
        except Py4JJavaError as e:
            self._geometry_api = "OGC"

        try:
            self._index_system = spark.conf.get("spark.databricks.mosaic.index.system")
        except Py4JJavaError as e:
            self._index_system = "H3"

        IndexSystemClass = getattr(mosaicPackageObject, self._index_system)
        GeometryAPIClass = getattr(mosaicPackageObject, self._geometry_api)

        self._context = MosaicContextClass.build(IndexSystemClass(), GeometryAPIClass())

    def invoke_function(self, name: str, *args: Any) -> MosaicColumn:
        func = getattr(self._context.functions(), name)
        return MosaicColumn(func(*args))

    @property
    def geometry_api(self):
        return self._geometry_api

    @property
    def index_system(self):
        return self._index_system
