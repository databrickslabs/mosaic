import os
import site
import sys
from typing import Any

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.column import Column as MosaicColumn


# jar_filename = "mosaic-1.0-SNAPSHOT-jar-with-dependencies.jar"

# db_jar_path = f"/databricks/python/lib/python{sys.version_info.major}.{sys.version_info.minor}/site-packages/mosaic/{jar_filename}"
# std_jar_path = os.path.join(site.getsitepackages()[0], "mosaic", jar_filename)
# if os.path.exists(db_jar_path):
#     jar_path = db_jar_path
# elif os.path.exists(std_jar_path):
#     jar_path = std_jar_path
# else:
#     raise FileNotFoundError(f"Mosaic JAR package {jar_filename} could not be located.")

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
# sc._jsc.addJar(jar_path)


MosaicContextClass = getattr(sc._jvm.com.databricks.mosaic.functions, "MosaicContext")
mosaicPackageRef = getattr(sc._jvm.com.databricks.mosaic, "package$")
mosaicPackageObject = getattr(mosaicPackageRef, "MODULE$")
H3 = getattr(mosaicPackageObject, "H3")
OGC = getattr(mosaicPackageObject, "OGC")

mosaicContext = MosaicContextClass.apply(H3(), OGC())


def _mosaic_invoke_function(
    name: str, mosaic_context: "MosaicContext", *args: Any
) -> MosaicColumn:
    assert SparkContext._active_spark_context is not None
    func = getattr(mosaic_context.functions(), name)
    return MosaicColumn(func(*args))
