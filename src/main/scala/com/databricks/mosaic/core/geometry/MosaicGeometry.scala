package com.databricks.mosaic.core.geometry

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
}

object MosaicGeometry {

  def apply(coordinates: Seq[Coordinate]): MosaicGeometry = {
    val geometryFactory = new GeometryFactory
    val geom = geometryFactory.createPolygon(coordinates.toArray)
    MosaicGeometryJTS(geom)
  }

}