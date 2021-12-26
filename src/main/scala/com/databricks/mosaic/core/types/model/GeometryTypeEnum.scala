package com.databricks.mosaic.core.types.model

import java.util.Locale

object GeometryTypeEnum extends Enumeration {
  val POINT = 1
  val MULTIPOINT = 2
  val LINESTRING = 3
  val MULTILINESTRING = 4
  val POLYGON = 5
  val MULTIPOLYGON = 6
  def fromString(value: String): GeometryTypeEnum.Value =
      GeometryTypeEnum.values.find(_.toString == value.toUpperCase(Locale.ROOT))
        .getOrElse(throw new IllegalArgumentException(
        s"Invalid mode for geometry type: $value." +
        s" Must be one of ${GeometryTypeEnum.values.mkString(",")}")
        )
}
