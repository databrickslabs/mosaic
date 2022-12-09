package com.databricks.labs.mosaic.core

import org.apache.spark.sql.types._

/**
  * Contains definition of all Mosaic specific data types. It provides methods
  * for type inference over geometry columns.
  */
package object types {

    val HexType: DataType = new HexType()
    val JSONType: DataType = new JSONType()
    // Note InternalGeometryType depends on InternalCoordType
    // They have to be declared in this order.
    val InternalCoordType: DataType = ArrayType.apply(DoubleType)
    val BoundaryType: ArrayType = ArrayType(ArrayType(InternalCoordType))
    val HolesType: ArrayType = ArrayType(ArrayType(ArrayType(InternalCoordType)))
    val InternalGeometryType: DataType = new InternalGeometryType()

}
