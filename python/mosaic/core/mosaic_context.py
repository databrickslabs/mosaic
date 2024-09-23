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
        self._mosaicGDALObject = getattr(
            sc._jvm.com.databricks.labs.mosaic.gdal, "MosaicGDAL"
        )
        self._indexSystemFactory = getattr(
            sc._jvm.com.databricks.labs.mosaic.core.index, "IndexSystemFactory"
        )

        try:
            self._geometry_api = spark.conf.get(
                "spark.databricks.labs.mosaic.geometry.api"
            )
        except Py4JJavaError as e:
            self._geometry_api = "JTS"

        try:
            self._index_system = spark.conf.get(
                "spark.databricks.labs.mosaic.index.system"
            )
        except Py4JJavaError as e:
            self._index_system = "H3"

        try:
            self._raster_api = spark.conf.get("spark.databricks.labs.mosaic.raster.api")
        except Py4JJavaError as e:
            self._raster_api = "GDAL"

        # singleton on the java side
        # - access dynamically
        IndexSystem = self._indexSystemFactory.getIndexSystem(self._index_system)
        GeometryAPIClass = getattr(self._mosaicPackageObject, self._geometry_api)
        self._context = self._mosaicContextClass.build(IndexSystem, GeometryAPIClass())

    def jContext(self):
        """
        :return: dynamic getter for jvm MosaicContext object
        """
        return self._context

    def jContextReset(self):
        """
        Reset the MosaicContext jContext().
        - This requires a re-init essentially.
        - Needed sometimes for checkpointing.
        """
        self._mosaicContextClass.reset()
        IndexSystem = self._indexSystemFactory.getIndexSystem(self._index_system)
        GeometryAPIClass = getattr(self._mosaicPackageObject, self._geometry_api)
        self._context = self._mosaicContextClass.build(IndexSystem, GeometryAPIClass())

    def invoke_function(self, name: str, *args: Any) -> MosaicColumn:
        """
        use jvm context to invoke function.
        :param name: name of function.
        :param args: any passed args.
        :return: MosaicColumn.
        """
        func = getattr(self._context.functions(), name)
        return MosaicColumn(func(*args))

    def jRegister(self, spark: SparkSession):
        """
        Register SQL expressions.
        - the jvm functions for checkpointing handle after initial invoke
          by enable.py.
        :param spark: session to use.
        """
        optionClass = getattr(spark._sc._jvm.scala, "Option$")
        optionModule = getattr(optionClass, "MODULE$")
        self._context.register(spark._jsparkSession, optionModule.apply(None))

    def jResetCheckpoint(self, spark: SparkSession):
        """
        Go back to defaults.
        - spark conf unset for use checkpoint (off)
        - spark conf unset for checkpoint path
        :param spark: session to use.
        """
        self._mosaicGDALObject.resetCheckpoint(spark._jsparkSession)

    def jEnableGDAL(self, spark: SparkSession, with_checkpoint_path: str = None):
        """
        Enable GDAL, assumes regular enable already called.
        :param spark: session to use.
        :param with_checkpoint_path: optional checkpoint path, default is None.
        """
        if with_checkpoint_path:
            self._mosaicGDALObject.enableGDALWithCheckpoint(
                spark._jsparkSession, with_checkpoint_path
            )
        else:
            self._mosaicGDALObject.enableGDAL(spark._jsparkSession)

    def jUpdateCheckpointPath(self, spark: SparkSession, path: str):
        """
        Change the checkpoint location; does not adjust checkpoint on/off (stays as-is).
        :param spark: session to use.
        :param path: new path.
        """
        self._mosaicGDALObject.updateCheckpointPath(spark._jsparkSession, path)

    def jSetCheckpointOff(self, spark: SparkSession):
        """
        Turn off checkpointing.
        :param spark: session to use.
        """
        self._mosaicGDALObject.setCheckpointOff(spark._jsparkSession)

    def jSetCheckpointOn(self, spark: SparkSession):
        """
        Turn on checkpointing, will use the configured path.
        :param spark: session to use.
        """
        self._mosaicGDALObject.setCheckpointOn(spark._jsparkSession)

    #################################################################
    # PROPERTY ACCESSORS + GETTERS
    #################################################################

    @property
    def geometry_api(self):
        return self._geometry_api

    @property
    def index_system(self):
        return self._index_system

    def is_use_checkpoint(self) -> bool:
        return self._mosaicGDALObject.isUseCheckpoint()

    def get_checkpoint_path(self) -> str:
        return self._mosaicGDALObject.getCheckpointPath()

    def get_checkpoint_path_default(self) -> str:
        return self._mosaicGDALObject.getCheckpointPathDefault()

    def has_context(self) -> bool:
        return self._context is not None
