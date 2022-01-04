package com.databricks.mosaic.core.types.model

import java.util.Locale

object GeometryTypeEnum extends Enumeration {
  val POINT = Value(1, "POINT") 
  val MULTIPOINT = Value(2, "MULTIPOINT")
  val LINESTRING = Value(3, "LINESTRING")
  val MULTILINESTRING = Value(4, "MULTILINESTRING")
  val POLYGON = Value(5, "POLYGON")
  val MULTIPOLYGON = Value(6, "MULTIPOLYGON")
  def fromString(value: String): GeometryTypeEnum.Value =
      GeometryTypeEnum.values.find(_.toString == value.toUpperCase(Locale.ROOT))
        .getOrElse(throw new IllegalArgumentException(
        s"Invalid mode for geometry type: $value." +
        s" Must be one of ${GeometryTypeEnum.values.mkString(",")}")
        )
  def fromId(id: Int): GeometryTypeEnum.Value =
    GeometryTypeEnum.values.find(_.id == id)
      .getOrElse(GeometryTypeEnum.MULTIPOLYGON)
}
