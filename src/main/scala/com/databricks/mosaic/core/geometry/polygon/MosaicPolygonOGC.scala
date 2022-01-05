package com.databricks.mosaic.core.geometry.polygon

import com.databricks.mosaic.core.geometry.point.{MosaicPoint, MosaicPointOGC}
import com.esri.core.geometry.ogc.{OGCGeometry, OGCLineString, OGCPolygon}

case class MosaicPolygonOGC(polygon: OGCPolygon)
  extends MosaicPolygon {

  override def getBoundaryPoints: Seq[MosaicPoint] = {
    MosaicPolygonOGC.getPoints(polygon.exteriorRing())
  }

  override def getHolePoints: Seq[Seq[MosaicPoint]] = {
    for (i <- 0 until polygon.numInteriorRing())
      yield MosaicPolygonOGC.getPoints(polygon.interiorRingN(i))
  }

}

object MosaicPolygonOGC {

  def apply(ogcGeometry: OGCGeometry): MosaicPolygonOGC = {
    new MosaicPolygonOGC(ogcGeometry.asInstanceOf[OGCPolygon])
  }

  def getPoints(lineString: OGCLineString): Seq[MosaicPoint] = {
    for (i <- 0 until lineString.numPoints())
      yield MosaicPointOGC(lineString.pointN(i))
  }

}
