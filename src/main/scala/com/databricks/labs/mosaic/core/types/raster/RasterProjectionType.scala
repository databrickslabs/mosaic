package com.databricks.labs.mosaic.core.types.raster

import org.apache.spark.sql.types._

class RasterProjectionType extends StructType(Array(
  StructField("crs", StringType),
  StructField("transformationCoefficients", ArrayType.apply(DoubleType))
))
