package com.databricks.mosaic.core.geometry

import com.esri.core.geometry.ogc.{OGCGeometry, OGCMultiPolygon}

case class MosaicMultiPolygonOGC(multiPolygon: OGCMultiPolygon) extends MosaicMultiPolygon {

  override def getBoundaryPoints: Seq[MosaicPoint] = {
    val n = multiPolygon.numGeometries()
    val boundaries = for (i <- 0 until n)
      yield MosaicPolygonOGC(multiPolygon.geometryN(i)).getBoundaryPoints
    boundaries.reduce(_ ++ _)
  }

  override def getHolePoints: Seq[Seq[MosaicPoint]] = {
    val n = multiPolygon.numGeometries()
    val holeGroups = for (i <- 0 until n)
      yield MosaicPolygonOGC(multiPolygon.geometryN(i)).getHolePoints
    holeGroups.reduce(_ ++ _)
  }

}

object MosaicMultiPolygonOGC {

  def apply(multiPolygon: OGCGeometry): MosaicMultiPolygonOGC =
    new MosaicMultiPolygonOGC(multiPolygon.asInstanceOf[OGCMultiPolygon])

}
