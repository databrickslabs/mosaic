package com.databricks.labs.mosaic.core.types

import org.apache.spark.sql.types._

/**
  * Type definition for JSON encoding. JSON encoding is defined as (json:
  * string). This abstraction over StringType is needed to ensure matching can
  * distinguish between StringType (WKT) and JSONType (GEOJSON).
  */
class JSONType()
    extends StructType(
      Array(
        StructField("json", StringType)
      )
    ) {

    override def simpleString: String = "GEOJSON"

    override def typeName: String = "struct"

}
