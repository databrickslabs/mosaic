package com.databricks.labs.mosaic.core.types

import org.apache.spark.sql.types._

/**
  * Type definition for InternalGeometryType. InternalGeometryType is defined as
  * (typeName: string, boundary: array[array[internal_coord] ], holes:
  * array[array[array[internal_coord] ] ]).
  */
class InternalGeometryType
    extends StructType(
      Array(
        // StructField("typeName", StringType),
        StructField("type_id", IntegerType),
        StructField("srid", IntegerType),
        StructField("boundary", BoundaryType),
        StructField("holes", HolesType)
      )
    ) {

    override def simpleString: String = "COORDS"

    override def typeName: String = "struct"

}
