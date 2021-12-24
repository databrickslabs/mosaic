package com.databricks.mosaic.core.geometry

import com.uber.h3core.util.GeoCoord
import org.locationtech.jts.geom.{Coordinate, GeometryFactory}

trait MosaicGeometry {

  def toWKB: Array[Byte]

  def getAPI: String

  def getCentroid: MosaicPoint

  def getCoordinates: Seq[MosaicPoint]

  def isEmpty: Boolean

  def getBoundary: Seq[MosaicPoint]

  def getHoles: Seq[Seq[MosaicPoint]]

  def boundary: MosaicGeometry

  def buffer(distance: Double): MosaicGeometry

  def simplify(tolerance: Double): MosaicGeometry

  def intersection(other: MosaicGeometry): MosaicGeometry

  def equals(other: MosaicGeometry): Boolean

  def equals(other: java.lang.Object): Boolean

  override def hashCode: Int
}

object MosaicGeometry {

  def fromCoordinates(coordinates: Seq[Coordinate]): MosaicGeometry = {
    val geometryFactory = new GeometryFactory
    val geom = geometryFactory.createPolygon(coordinates.toArray)
    MosaicGeometryJTS(geom)
  }

  def fromGeoCoords(geoCoords: Seq[GeoCoord]): MosaicGeometry = {
    val coords = geoCoords.map(c => new Coordinate(c.lng, c.lat))
    this.fromCoordinates(coords)
  }

}
