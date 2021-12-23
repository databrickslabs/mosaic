package com.databricks.mosaic.core.geometry

import com.databricks.mosaic.expressions.format.Conversions
import org.locationtech.jts.geom.{Geometry, LinearRing, MultiPolygon, Polygon}
import org.locationtech.jts.simplify.DouglasPeuckerSimplifier

case class MosaicGeometryJTS(geom: Geometry)
  extends MosaicGeometry {

  override def toWKB: Array[Byte] = Conversions.geom2wkb(geom)

  override def getAPI: String = "JTS"

  override def getCentroid: MosaicPoint = MosaicPointJTS(geom.getCentroid)

  override def getCoordinates: Seq[MosaicPoint] = {
    geom.getCoordinates.map(MosaicPointJTS(_))
  }

  override def simplify(tolerance: Double = 1e-8): MosaicGeometry = MosaicGeometryJTS(
    DouglasPeuckerSimplifier.simplify(geom, tolerance)
  )

  override def isEmpty: Boolean = geom.isEmpty

  override def getBoundary: Seq[MosaicPoint] = {
    geom.getGeometryType match {
      case "LinearRing" => geom.asInstanceOf[LinearRing].getCoordinates.map(MosaicPointJTS(_)).toList
      case "Polygon" => MosaicPolygonJTS(geom.asInstanceOf[Polygon]).getBoundaryPoints
      case "MultiPolygon" => MosaicMultiPolygonJTS(geom.asInstanceOf[MultiPolygon]).getBoundaryPoints
    }
  }

  override def getHoles: Seq[Seq[MosaicPoint]] = {
    geom.getGeometryType match {
      case "LinearRing" => Seq(geom.asInstanceOf[LinearRing].getCoordinates.map(MosaicPointJTS(_)).toList)
      case "Polygon" => MosaicPolygonJTS(geom.asInstanceOf[Polygon]).getHolePoints
      case "MultiPolygon" => MosaicMultiPolygonJTS(geom.asInstanceOf[MultiPolygon]).getHolePoints
    }
  }

  override def boundary: MosaicGeometry = MosaicGeometryJTS(geom.getBoundary)

  override def buffer(distance: Double): MosaicGeometry = MosaicGeometryJTS(geom.buffer(distance))

  override def intersection(other: MosaicGeometry): MosaicGeometry = {
    val otherGeom = other.asInstanceOf[MosaicGeometryJTS].geom
    val intersection = this.geom.intersection(otherGeom)
    MosaicGeometryJTS(intersection)
  }

  override def equals(other: MosaicGeometry): Boolean = {
    val otherGeom = other.asInstanceOf[MosaicGeometryJTS].geom
    this.geom.equalsExact(otherGeom)
  }

}