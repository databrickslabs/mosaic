import os
import sys

from pyspark.sql import SparkSession


spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

jar_filename = "mosaic-1.0-SNAPSHOT-jar-with-dependencies.jar"
db_jar_path = f"/databricks/python/lib/python{sys.version_info.major}.{sys.version_info.minor}/site-packages/mosaic/{jar_filename}"
if os.path.exists(db_jar_path):
    jar_path = db_jar_path
else:
    raise FileNotFoundError(f"Mosaic JAR package {jar_filename} could not be located.")

JavaURI = getattr(sc._jvm.java.net, "URI")
JavaJarId = getattr(sc._jvm.com.databricks.libraries, "JavaJarId")
ManagedLibraryId = getattr(sc._jvm.com.databricks.libraries, "ManagedLibraryId")
ManagedLibraryVersions = getattr(sc._jvm.com.databricks.libraries, "ManagedLibraryVersions")
NoVersion = getattr(ManagedLibraryVersions, "NoVersion$")
NoVersionModule = getattr(NoVersion, "MODULE$")
DatabricksILoop = getattr(sc._jvm.com.databricks.backend.daemon.driver, "DatabricksILoop")
converters = sc._jvm.scala.collection.JavaConverters

JarURI = JavaURI.create("file:" + jar_path)
lib = JavaJarId(JarURI, ManagedLibraryId.defaultOrganization(), NoVersionModule.simpleString())

libSeq = converters.asScalaBufferConverter((lib, )).asScala().toSeq()

context = DatabricksILoop.getSharedDriverContextIfExists().get()
context.registerNewLibraries(libSeq)
context.attachLibrariesToSpark(libSeq)
