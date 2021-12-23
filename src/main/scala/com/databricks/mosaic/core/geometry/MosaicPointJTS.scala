package com.databricks.mosaic.core.geometry

import com.uber.h3core.util.GeoCoord
import org.locationtech.jts.geom.{Coordinate, GeometryFactory, Point}

case class MosaicPointJTS(point: Point) extends MosaicPoint {

  override def getX: Double = point.getX

  override def getY: Double = point.getY

  override def getZ: Double = point.getCoordinate.z

  override def distance(other: MosaicPoint): Double = {
    val otherPoint = other.asInstanceOf[MosaicPointJTS].point
    point.distance(otherPoint)
  }

  override def geoCoord: GeoCoord = new GeoCoord(point.getY, point.getX)

}

object MosaicPointJTS {

  def apply(coord: Coordinate): MosaicPointJTS = {
    val gf = new GeometryFactory()
    MosaicPointJTS(gf.createPoint(coord))
  }

}
