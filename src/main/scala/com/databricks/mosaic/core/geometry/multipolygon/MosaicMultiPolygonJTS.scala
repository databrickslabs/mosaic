package com.databricks.mosaic.core.geometry.multipolygon

import com.databricks.mosaic.core.geometry.point.MosaicPoint
import com.databricks.mosaic.core.geometry.polygon.MosaicPolygonJTS
import org.locationtech.jts.geom.{Geometry, MultiPolygon, Polygon}

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

object MosaicMultiPolygonJTS {

  def apply(multiPolygon: Geometry) = new MosaicMultiPolygonJTS(multiPolygon.asInstanceOf[MultiPolygon])

}
