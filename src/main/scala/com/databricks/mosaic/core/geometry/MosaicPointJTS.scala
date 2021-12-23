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

  override def coord: Coordinate = new Coordinate(point.getX, point.getY)
}

object MosaicPointJTS {

  def apply(geoCoord: GeoCoord): MosaicPointJTS = {
    this.apply(new Coordinate(geoCoord.lng, geoCoord.lat))
  }

  def apply(coord: Coordinate): MosaicPointJTS = {
    val gf = new GeometryFactory()
    MosaicPointJTS(gf.createPoint(coord))
  }

}
