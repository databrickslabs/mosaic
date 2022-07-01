package com.databricks.labs.mosaic.core.types

import org.apache.spark.sql.types._

/**
  * Type definition for Hex encoding. Hex encoding is defined as (hex: string).
  * This abstraction over StringType is needed to ensure matching can
  * distinguish between StringType (WKT) and HexType (HEX).
  */
class HexType()
    extends StructType(
      Array(
        StructField("hex", StringType)
      )
    ) {

    override def simpleString: String = "HEX"

    override def typeName: String = "struct"

}
