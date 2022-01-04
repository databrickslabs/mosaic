package com.databricks.mosaic.core.geometry.polygon

import com.databricks.mosaic.core.geometry.point.{MosaicPoint, MosaicPointJTS}
import org.locationtech.jts.geom.{Geometry, LinearRing, Polygon}

case class MosaicPolygonJTS(polygon: Polygon)
  extends MosaicPolygon {

  override def getBoundaryPoints: Seq[MosaicPoint] = {
    val exteriorRing = polygon.getBoundary.getGeometryN(0)
    MosaicPolygonJTS.getPoints(exteriorRing.asInstanceOf[LinearRing])
  }

  override def getHolePoints: Seq[Seq[MosaicPoint]] = {
    val boundary = polygon.getBoundary
    val m = boundary.getNumGeometries
    val holes = for (i <- 1 until m) yield boundary.getGeometryN(i).asInstanceOf[LinearRing]
    holes.map(MosaicPolygonJTS.getPoints)
  }

}

object MosaicPolygonJTS {

  def apply(geometry: Geometry): MosaicPolygonJTS = {
    new MosaicPolygonJTS(geometry.asInstanceOf[Polygon])
  }

  def getPoints(linearRing: LinearRing): Seq[MosaicPoint] = {
    linearRing.getCoordinates.map(MosaicPointJTS(_))
  }

}
