package com.databricks.labs.mosaic.core.types.raster

import org.apache.spark.sql.types._

class RasterBandMetadataType extends StructType(Array(
  StructField("index", IntegerType),
  StructField("description", StringType),
  StructField("units", StringType),
  StructField("dataType", IntegerType),
  StructField("minValue", DoubleType),
  StructField("maxValue", DoubleType),
  StructField("valueScale", DoubleType),
  StructField("valueOffset", DoubleType),
))
