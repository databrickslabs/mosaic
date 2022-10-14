import importlib.metadata
import importlib.resources
import os

from py4j.protocol import Py4JJavaError


class MosaicLibraryHandler:
    spark = None
    sc = None
    _jar_path = None
    _jar_filename = f"mosaic-{importlib.metadata.version('databricks-mosaic')}-jar-with-dependencies.jar"
    _auto_attached_enabled = None

    def __init__(self, spark):
        self.spark = spark
        self.sc = spark.sparkContext
        self.sc.setLogLevel("info")
        log4jLogger = self.sc._jvm.org.apache.log4j
        LOGGER = log4jLogger.LogManager.getLogger(__class__.__name__)

        if self.auto_attach_enabled:
            LOGGER.info(f"Looking for Mosaic JAR at {self.mosaic_library_location}.")
            if not os.path.exists(self.mosaic_library_location):
                raise FileNotFoundError(
                    f"Mosaic JAR package {self._jar_filename} could not be located at {self.mosaic_library_location}."
                )
            LOGGER.info(f"Automatically attaching Mosaic JAR to cluster.")
            self.auto_attach()

    @property
    def auto_attach_enabled(self) -> bool:
        if not self._auto_attached_enabled:
            try:
                result = (
                    self.spark.conf.get("spark.databricks.labs.mosaic.jar.autoattach")
                    == "true"
                )
            except Py4JJavaError as e:
                result = True
            self._auto_attached_enabled = result
        return self._auto_attached_enabled

    @property
    def mosaic_library_location(self):
        if not self._jar_path:
            try:
                self._jar_path = self.spark.conf.get(
                    "spark.databricks.labs.mosaic.jar.path"
                )
                self._jar_filename = self._jar_path.split("/")[-1]
            except Py4JJavaError as e:
                with importlib.resources.path("mosaic.lib", self._jar_filename) as p:
                    self._jar_path = p.as_posix()
        return self._jar_path

    def auto_attach(self):
        JavaURI = getattr(self.sc._jvm.java.net, "URI")
        JavaJarId = getattr(self.sc._jvm.com.databricks.libraries, "JavaJarId")
        ManagedLibraryId = getattr(
            self.sc._jvm.com.databricks.libraries, "ManagedLibraryId"
        )
        ManagedLibraryVersions = getattr(
            self.sc._jvm.com.databricks.libraries, "ManagedLibraryVersions"
        )
        NoVersion = getattr(ManagedLibraryVersions, "NoVersion$")
        NoVersionModule = getattr(NoVersion, "MODULE$")
        DatabricksILoop = getattr(
            self.sc._jvm.com.databricks.backend.daemon.driver, "DatabricksILoop"
        )
        converters = self.sc._jvm.scala.collection.JavaConverters

        JarURI = JavaURI.create("file:" + self._jar_path)
        lib = JavaJarId(
            JarURI,
            ManagedLibraryId.defaultOrganization(),
            NoVersionModule.simpleString(),
        )
        libSeq = converters.asScalaBufferConverter((lib,)).asScala().toSeq()

        context = DatabricksILoop.getSharedDriverContextIfExists().get()
        context.registerNewLibraries(libSeq)
        context.attachLibrariesToSpark(libSeq)
