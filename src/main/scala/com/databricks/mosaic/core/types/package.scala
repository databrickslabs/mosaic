package com.databricks.mosaic.core

import com.databricks.mosaic.expressions.format.Conversions
import com.databricks.mosaic.core.types.model.InternalGeometry
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.locationtech.jts.geom.Geometry

/**
 * Contains definition of all Mosaic specific data types.
 * It provides methods for type inference over geometry columns.
 */
package object types {
  val ChipType: DataType = new ChipType()
  val MosaicType: DataType = new MosaicType()
  val HexType: DataType = new HexType()
  val JSONType: DataType = new JSONType()
  // Note InternalGeometryType depends on InternalCoordType
  // They have to be declared in this order.
  val InternalCoordType: DataType = ArrayType.apply(DoubleType)
  val InternalGeometryType: DataType = new InternalGeometryType()

  /**
   * Constructs an instance of [[Geometry]] based on an instance
   * of spark internal data.
   * @param inputData An instance of [[InternalRow]].
   * @param geomType A data type of the geometry.
   * @return An instance of [[Geometry]].
   */
  def struct2geom(inputData: InternalRow, geomType: DataType): Geometry = {
    geomType match {
      case _: BinaryType => Conversions.typed.wkb2geom(inputData.getBinary(0))
      case _: StringType => Conversions.typed.wkt2geom(inputData.getString(0))
      case _: HexType => Conversions.hex2geom(inputData.get(0, HexType))
      case _: JSONType => Conversions.geojson2geom(inputData.get(0, JSONType))
      case _: InternalGeometryType => InternalGeometry.apply(
        inputData.get(0, InternalGeometryType).asInstanceOf[InternalRow]
      ).toGeom
    }
  }

  /**
   * Constructs an instance of [[Geometry]] based on Any instance
   * coming from spark nullSafeEval method.
   * @param inputData An instance of [[InternalRow]].
   * @param geomType A data type of the geometry.
   * @return An instance of [[Geometry]].
   */
  def any2geometry(inputData: Any, geomType: DataType): Geometry = {
    geomType match {
      case _: BinaryType => Conversions.wkb2geom(inputData)
      case _: StringType => Conversions.wkt2geom(inputData)
      case _: HexType => Conversions.hex2geom(inputData)
      case _: JSONType => Conversions.geojson2geom(inputData)
      case _: InternalGeometryType => InternalGeometry(inputData.asInstanceOf[InternalRow]).toGeom
    }
  }
}
