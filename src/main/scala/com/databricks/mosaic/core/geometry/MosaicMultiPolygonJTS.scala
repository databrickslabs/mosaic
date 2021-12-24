package com.databricks.mosaic.core.geometry

import org.locationtech.jts.geom.{MultiPolygon, Polygon}

case class MosaicMultiPolygonJTS(multiPolygon: MultiPolygon) extends MosaicMultiPolygon {

  override def getBoundaryPoints: Seq[MosaicPoint] = {
    val n = multiPolygon.getNumGeometries
    val boundaries = for (i <- 0 until n)
      yield {
        val polygon = MosaicPolygonJTS(multiPolygon.getGeometryN(i).asInstanceOf[Polygon])
        polygon.getBoundaryPoints
      }
    boundaries.reduce(_ ++ _)
  }

  override def getHolePoints: Seq[Seq[MosaicPoint]] = {
    val n = multiPolygon.getNumGeometries
    val holeGroups = for (i <- 0 until n)
      yield {
        val polygon = MosaicPolygonJTS(multiPolygon.getGeometryN(i).asInstanceOf[Polygon])
        polygon.getHolePoints
      }
    val holePoints = holeGroups.reduce(_ ++ _)
    holePoints
  }

}
