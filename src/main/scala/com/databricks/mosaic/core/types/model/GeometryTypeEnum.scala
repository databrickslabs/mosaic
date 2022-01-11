package com.databricks.mosaic.core.types.model

import java.util.Locale

object GeometryTypeEnum extends Enumeration {
  val POINT: GeometryTypeEnum.Value = Value(1, "POINT")
  val MULTIPOINT: GeometryTypeEnum.Value = Value(2, "MULTIPOINT")
  val LINESTRING: GeometryTypeEnum.Value = Value(3, "LINESTRING")
  val MULTILINESTRING: GeometryTypeEnum.Value = Value(4, "MULTILINESTRING")
  val POLYGON: GeometryTypeEnum.Value = Value(5, "POLYGON")
  val MULTIPOLYGON: GeometryTypeEnum.Value = Value(6, "MULTIPOLYGON")
  // coercion type JTS boundary returns LinearRing instead of LineString
  val LINEARRING: GeometryTypeEnum.Value = Value(7, "LINEARRING")
  val GEOMETRYCOLLECTION: GeometryTypeEnum.Value = Value(8, "GEOMETRYCOLLECTION")

  def fromString(value: String): GeometryTypeEnum.Value =
    GeometryTypeEnum.values
      .find(_.toString == value.toUpperCase(Locale.ROOT))
      .getOrElse(
        throw new IllegalArgumentException(
          s"Invalid mode for geometry type: $value." +
            s" Must be one of ${GeometryTypeEnum.values.mkString(",")}"
        )
      )

  def fromId(id: Int): GeometryTypeEnum.Value =
    GeometryTypeEnum.values
      .find(_.id == id)
      .getOrElse(throw new IllegalArgumentException(s"Invalid value for geometry type id: $id."))
}
