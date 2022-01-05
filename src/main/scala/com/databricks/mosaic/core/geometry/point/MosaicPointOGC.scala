package com.databricks.mosaic.core.geometry.point

import com.esri.core.geometry.ogc.{OGCGeometry, OGCPoint}
import com.esri.core.geometry.{Point, SpatialReference}
import com.uber.h3core.util.GeoCoord
import org.locationtech.jts.geom.Coordinate

case class MosaicPointOGC(point: OGCPoint) extends MosaicPoint  {

  override def getX: Double = point.X()

  override def getY: Double = point.Y()

  override def getZ: Double = point.Z()

  override def distance(other: MosaicPoint): Double = {
    val otherPoint = other.asInstanceOf[MosaicPointOGC].point
    point.distance(otherPoint)
  }

  override def geoCoord: GeoCoord = new GeoCoord(point.Y(), point.X())

  override def coord: Coordinate = new Coordinate(point.X(), point.Y())

  override def asSeq: Seq[Double] = if (point.is3D()) {
    Seq(getX, getY, getZ)
  } else {
    Seq(getX, getY)
  }
}

object MosaicPointOGC {

  def apply(geoCoord: GeoCoord): MosaicPointOGC = {
    MosaicPointOGC(
      new OGCPoint(new Point(geoCoord.lng, geoCoord.lat), SpatialReference.create(4326))
    )
  }

  def apply(point: OGCGeometry): MosaicPointOGC = new MosaicPointOGC(point.asInstanceOf[OGCPoint])

}
