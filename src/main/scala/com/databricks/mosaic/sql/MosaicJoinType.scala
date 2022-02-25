//package com.databricks.mosaic.sql
//
//import java.util.Locale
//
//object MosaicJoinType extends Enumeration {
//  val POINT_IN_POLYGON: MosaicJoinType.Value = Value(1, "PointInPolygon")
//  val POLYGON_INTERSECTION: MosaicJoinType.Value = Value(2, "PolygonIntersection")
//
//  def fromString(value: String): MosaicJoinType.Value =
//    MosaicJoinType.values
//      .find(_.toString.toUpperCase(Locale.ROOT) == value.toUpperCase(Locale.ROOT))
//      .getOrElse(
//        throw new IllegalArgumentException(
//          s"Invalid mode for geometry type: $value." +
//            s" Must be one of ${MosaicJoinType.values.mkString(",")}"
//        )
//      )
//
//  def fromId(id: Int): MosaicJoinType.Value =
//    MosaicJoinType.values
//      .find(_.id == id)
//      .getOrElse(throw new IllegalArgumentException(s"Invalid value for mosaic join type id: $id."))
//
//}
