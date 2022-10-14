package com.databricks.labs.mosaic.core.types

import org.apache.spark.sql.types._

/**
  * Type definition for MosaicType. MosaicType is defined as (chips:
  * array[chip]).
  */
class MosaicType(fields: Array[StructField]) extends StructType(fields) {

    override def simpleString: String = "MOSAIC"

    override def typeName: String = "struct"

}

object MosaicType {
    def apply(idType: DataType): StructType = {
        new MosaicType(
          Array(
            StructField("chips", ArrayType(ChipType(idType)))
          )
        )
    }
}
