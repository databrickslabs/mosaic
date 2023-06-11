package com.databricks.labs.mosaic.core.types

import java.util.Locale
import scala.collection.immutable

/**
 * Enumeration of geometry types supported by Mosaic.
 */
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

  val pointGeometries: Seq[GeometryTypeEnum.Value] = List(this.POINT, this.MULTIPOINT)
  val linestringGeometries: Seq[GeometryTypeEnum.Value] = List(this.LINESTRING, this.MULTILINESTRING)
  val polygonGeometries: immutable.Seq[GeometryTypeEnum.Value] = List(this.POLYGON, this.MULTIPOLYGON)

  val singleGeometries: Seq[GeometryTypeEnum.Value] = List(this.POINT, this.LINESTRING, this.POLYGON)
  val multipleGeometries: Seq[GeometryTypeEnum.Value] = List(this.MULTIPOINT, this.MULTILINESTRING, this.MULTIPOLYGON, this.GEOMETRYCOLLECTION)

  def fromString(value: String): GeometryTypeEnum.Value =
    GeometryTypeEnum.values
      .find(_.toString == value.toUpperCase(Locale.ROOT))
      .getOrElse(
        throw new Error(
          s"Invalid mode for geometry type: $value." +
            s" Must be one of ${GeometryTypeEnum.values.mkString(",")}"
        )
      )

  def fromId(id: Int): GeometryTypeEnum.Value =
    GeometryTypeEnum.values
      .find(_.id == id)
      .getOrElse(throw new Error(s"Invalid value for geometry type id: $id."))

  def groupOf(enumerator: GeometryTypeEnum.Value): GeometryTypeEnum.Value =
    enumerator match {
      case g if pointGeometries.contains(g) => this.POINT
      case g if linestringGeometries.contains(g) => this.LINESTRING
      case g if polygonGeometries.contains(g) => this.POLYGON
      case _ => this.GEOMETRYCOLLECTION
    }

  def isFlat(enumerator: GeometryTypeEnum.Value): Boolean =
    enumerator match {
      case g if singleGeometries.contains(g) => true
      case g if multipleGeometries.contains(g) => false
    }

}
