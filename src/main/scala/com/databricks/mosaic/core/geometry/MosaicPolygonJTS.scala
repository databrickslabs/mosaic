package com.databricks.mosaic.core.geometry

import org.locationtech.jts.geom.Polygon

case class MosaicPolygonJTS(polygon: Polygon)
  extends MosaicPolygon {

  override def getBoundaryPoints: Seq[MosaicPoint] = {
    val exteriorRing = polygon.getBoundary.getGeometryN(0)
    val points = exteriorRing.getCoordinates.toList
      .map(MosaicPointJTS(_))
    points
  }

  override def getHolePoints: Seq[Seq[MosaicPoint]] = {
    val boundary = polygon.getBoundary
    val m = boundary.getNumGeometries
    val holes = for (i <- 1 until m) yield boundary.getGeometryN(i)
    holes.map(MosaicGeometryJTS(_).getBoundary)
  }

}
