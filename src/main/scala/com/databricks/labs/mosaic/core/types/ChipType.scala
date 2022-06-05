package com.databricks.labs.mosaic.core.types

import org.apache.spark.sql.types._

/**
  * Type definition for Chip. Chip is defined as (is_core: boolean, h3: long,
  * wkb: binary).
  */
class ChipType()
    extends StructType(
      Array(
        StructField("is_core", BooleanType),
        StructField("index_id", LongType),
        StructField("wkb", BinaryType)
      )
    ) {
    override def simpleString: String = "CHIP"
}
