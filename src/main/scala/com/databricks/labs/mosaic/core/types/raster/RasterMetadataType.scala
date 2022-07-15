package com.databricks.labs.mosaic.core.types.raster

 import com.databricks.labs.mosaic.core.types._
import org.apache.spark.sql.types._

class RasterMetadataType extends StructType(Array(
  StructField("xSize", IntegerType),
  StructField("ySize", IntegerType),
  StructField("extent", RasterExtentType),
  StructField("numBands", IntegerType),
  StructField("projection", RasterProjectionType),
  StructField("bands", ArrayType.apply(RasterBandType))
))



