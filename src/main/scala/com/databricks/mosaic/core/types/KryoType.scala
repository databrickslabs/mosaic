package com.databricks.mosaic.core.types

import org.apache.spark.sql.types.{BinaryType, IntegerType, StructField, StructType}

class KryoType() extends StructType(
  Array(
    StructField("type_id", IntegerType),
    StructField("kryo", BinaryType)
  )
) {
  override def typeName: String = "kryo"
}
