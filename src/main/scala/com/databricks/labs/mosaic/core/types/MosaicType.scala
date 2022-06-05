package com.databricks.labs.mosaic.core.types

import org.apache.spark.sql.types._

/**
  * Type definition for MosaicType. MosaicType is defined as (chips:
  * array[chip]).
  */
class MosaicType()
    extends StructType(
      Array(
        StructField("chips", ArrayType(ChipType))
      )
    ) {
    override def simpleString: String = "MOSAIC"
}
