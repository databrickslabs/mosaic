package com.databricks.labs.mosaic.core

//import com.databricks.labs.mosaic.core.types.CDMAttributeType

import org.apache.spark.sql.types._

/**
  * Contains definition of all Mosaic specific data types. It provides methods
  * for type inference over geometry columns.
  */
package object types {

    val ChipType: DataType = new ChipType()
    val MosaicType: DataType = new MosaicType()
    val HexType: DataType = new HexType()
    val JSONType: DataType = new JSONType()
    // Note InternalGeometryType depends on InternalCoordType
    // They have to be declared in this order.
    val InternalCoordType: DataType = ArrayType.apply(DoubleType)
    val BoundaryType: ArrayType = ArrayType(ArrayType(InternalCoordType))
    val HolesType: ArrayType = ArrayType(ArrayType(ArrayType(InternalCoordType)))
    val InternalGeometryType: DataType = new InternalGeometryType()
    val KryoType: DataType = new KryoType()

}
