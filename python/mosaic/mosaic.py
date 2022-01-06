from typing import Any

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.column import Column as MosaicColumn


spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

MosaicContextClass = getattr(sc._jvm.com.databricks.mosaic.functions, "MosaicContext")
mosaicPackageRef = getattr(sc._jvm.com.databricks.mosaic, "package$")
mosaicPackageObject = getattr(mosaicPackageRef, "MODULE$")
H3 = getattr(mosaicPackageObject, "H3")
OGC = getattr(mosaicPackageObject, "OGC")

mosaicContext = MosaicContextClass.apply(H3(), OGC())


def _mosaic_invoke_function(name: str, mosaic_context: "MosaicContext", *args: Any) -> MosaicColumn:
  assert SparkContext._active_spark_context is not None
  func = getattr(mosaic_context.functions(), name)
  return MosaicColumn(func(*args))
