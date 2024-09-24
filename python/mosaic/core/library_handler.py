import importlib.metadata
import importlib.resources
import os

from py4j.protocol import Py4JJavaError


class MosaicLibraryHandler:
    spark = None
    sc = None
    _jar_path = None
    _jar_filename = None
    _auto_attached_enabled = None

    def __init__(self, spark, log_info: bool = True):
        self.spark = spark
        self.sc = spark.sparkContext
        LOGGER = None
        if log_info:
            log4jLogger = self.sc._jvm.org.apache.log4j
            LOGGER = log4jLogger.LogManager.getLogger(__class__.__name__)

        if self.auto_attach_enabled:
            jar_path = self.mosaic_library_location
            LOGGER and LOGGER.info(f"Looking for Mosaic JAR at {jar_path}.")
            if not os.path.exists(jar_path):
                raise FileNotFoundError(
                    f"Mosaic JAR package {self._jar_filename} could not be located at {jar_path}."
                )
            LOGGER and LOGGER.info(f"Automatically attaching Mosaic JAR to cluster.")
            self.auto_attach()

    @property
    def auto_attach_enabled(self) -> bool:
        if self._auto_attached_enabled is None:
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
        if self._jar_path is None:
            try:
                self._jar_path = self.spark.conf.get(
                    "spark.databricks.labs.mosaic.jar.path"
                )
                self._jar_filename = self._jar_path.split("/")[-1]
            except Py4JJavaError as e:
                self._jar_filename = f"mosaic-{importlib.metadata.version('databricks-mosaic')}-jar-with-dependencies.jar"
                try:
                    with importlib.resources.path(
                        "mosaic.lib", self._jar_filename
                    ) as p:
                        self._jar_path = p.as_posix()
                except FileNotFoundError as fnf:
                    self._jar_filename = f"mosaic-{importlib.metadata.version('databricks-mosaic')}-SNAPSHOT-jar-with-dependencies.jar"
                    with importlib.resources.path(
                        "mosaic.lib", self._jar_filename
                    ) as p:
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

        try:
            # This will fix the exception when running on Databricks Runtime 13.x+
            optionClass = getattr(self.sc._jvm.scala, "Option$")
            optionModule = getattr(optionClass, "MODULE$")
            lib = JavaJarId(
                JarURI,
                ManagedLibraryId.defaultOrganization(),
                NoVersionModule.simpleString(),
                optionModule.apply(None),
                optionModule.apply(None),
            )
        except:
            lib = JavaJarId(
                JarURI,
                ManagedLibraryId.defaultOrganization(),
                NoVersionModule.simpleString(),
            )

        libSeq = converters.asScalaBufferConverter((lib,)).asScala().toSeq()

        context = DatabricksILoop.getSharedDriverContextIfExists().get()
        context.registerNewLibraries(libSeq)
        context.attachLibrariesToSpark(libSeq)
