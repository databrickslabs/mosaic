package com.databricks.labs.mosaic.core.types

import org.apache.spark.sql.types._

/**
  * Type definition for Chip. Chip is defined as (is_core: boolean, index_id:
  * long, wkb: binary).
  */
class RasterChipType(fields: Array[StructField]) extends StructType(fields) {

    override def simpleString: String = "RASTER_CHIP"

    override def typeName: String = "struct"

}

object RasterChipType {

    def apply(idType: DataType): ChipType = {
        require(Seq(LongType, IntegerType, StringType).contains(idType))
        new ChipType(
          Array(
            StructField("index_id", idType),
            StructField("raster", BinaryType)
          )
        )
    }

}
