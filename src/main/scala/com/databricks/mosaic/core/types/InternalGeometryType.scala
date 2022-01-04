package com.databricks.mosaic.core.types

import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}

/**
 * Type definition for InternalGeometryType.
 * InternalGeometryType is defined as
 * (typeName: string, boundary: array[array[internal_coord] ],
 *  holes: array[array[array[internal_coord] ] ]).
 */
class InternalGeometryType extends StructType(
  Array(
    // StructField("typeName", StringType),
    StructField("typeId", IntegerType),
    StructField("boundary", ArrayType(ArrayType(InternalCoordType))),
    StructField("holes", ArrayType(ArrayType(ArrayType(InternalCoordType))))
  )
) {
  override def typeName: String = "internal_geometry"
}
