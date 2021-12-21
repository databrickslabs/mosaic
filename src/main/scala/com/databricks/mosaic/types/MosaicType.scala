package com.databricks.mosaic.types

import org.apache.spark.sql.types.{ArrayType, StructField, StructType}

/**
 * Type definition for MosaicType.
 * MosaicType is defined as (chips: array[chip]).
 */
class MosaicType() extends StructType(
  Array(
    StructField("chips", ArrayType(ChipType))
  )
) {
  override def typeName: String = "mosaic"
}