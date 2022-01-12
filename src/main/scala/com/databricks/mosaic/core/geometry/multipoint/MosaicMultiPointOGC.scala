package com.databricks.mosaic.core.geometry.multipoint

import com.esri.core.geometry.{MultiPoint, OperatorConvexHull, Point}
import com.esri.core.geometry.ogc.{OGCGeometry, OGCMultiPoint, OGCPolygon}

import org.apache.spark.sql.catalyst.InternalRow

import com.databricks.mosaic.core.geometry.{GeometryReader, MosaicGeometry, MosaicGeometryOGC}
import com.databricks.mosaic.core.geometry.point.{MosaicPoint, MosaicPointOGC}
import com.databricks.mosaic.core.geometry.polygon.MosaicPolygon
import com.databricks.mosaic.core.types.model.{GeometryTypeEnum, InternalCoord, InternalGeometry}
import com.databricks.mosaic.core.types.model.GeometryTypeEnum.MULTIPOINT
import com.databricks.mosaic.core.geometry.polygon.MosaicPolygonOGC

class MosaicMultiPointOGC(multiPoint: OGCMultiPoint)
  extends MosaicGeometryOGC(multiPoint) with MosaicMultiPoint {

  override def toInternal: InternalGeometry = {
    val points = asSeq.map(_.coord).map(InternalCoord(_))
    new InternalGeometry(MULTIPOINT.id, Array(points.toArray), Array(Array(Array())))
  }

  override def asSeq: Seq[MosaicPoint] = {
    for (i <- 0 until multiPoint.numGeometries())
      yield MosaicPointOGC(multiPoint.geometryN(i))
  }

  override def getBoundary: Seq[MosaicPoint] = asSeq

  override def getHoles: Seq[Seq[MosaicPoint]] = Nil

  override def getLength: Double = 0.0

  override def flatten: Seq[MosaicGeometry] = asSeq

  override def convexHull: MosaicPolygon = {
    new MosaicPolygonOGC(multiPoint.convexHull.asInstanceOf[OGCPolygon])
}
}

object MosaicMultiPointOGC extends GeometryReader {

  def apply(multiPoint: OGCGeometry): MosaicGeometryOGC =
    new MosaicMultiPointOGC(multiPoint.asInstanceOf[OGCMultiPoint])

  //noinspection ZeroIndexToHead
  override def fromInternal(row: InternalRow): MosaicGeometry = {
    val internalGeom = InternalGeometry(row)
    require(internalGeom.typeId == MULTIPOINT.id)

    val multiPoint = new MultiPoint()
    val coordsCollection = internalGeom.boundaries.head.map(_.coords)
    val dim = coordsCollection.head.length

    dim match {
      case 2 => coordsCollection.foreach(coords => multiPoint.add(new Point(coords(0), coords(1))))
      case 3 => coordsCollection.foreach(coords => multiPoint.add(new Point(coords(0), coords(1), coords(2))))
      case _ => throw new UnsupportedOperationException("Only 2D and 3D points supported.")
    }

    val ogcMultiPoint = new OGCMultiPoint(multiPoint, MosaicGeometryOGC.spatialReference)
    new MosaicMultiPointOGC(ogcMultiPoint)
  }

  //noinspection ZeroIndexToHead
  override def fromPoints(points: Seq[MosaicPoint], geomType: GeometryTypeEnum.Value = MULTIPOINT): MosaicGeometry = {
    require(geomType.id == MULTIPOINT.id)

    val multiPoint = new MultiPoint()
    val dim = points.head.asSeq.length

    dim match {
      case 2 => points.foreach(point => multiPoint.add(new Point(point.asSeq(0), point.asSeq(1))))
      case 3 => points.foreach(point => multiPoint.add(new Point(point.asSeq(0), point.asSeq(1), point.asSeq(2))))
      case _ => throw new UnsupportedOperationException("Only 2D and 3D points supported.")
    }

    val ogcMultiPoint = new OGCMultiPoint(multiPoint, MosaicGeometryOGC.spatialReference)
    new MosaicMultiPointOGC(ogcMultiPoint)
  }

  override def fromWKB(wkb: Array[Byte]): MosaicGeometry = MosaicGeometryOGC.fromWKB(wkb)

  override def fromWKT(wkt: String): MosaicGeometry = MosaicGeometryOGC.fromWKT(wkt)

  override def fromJSON(geoJson: String): MosaicGeometry = MosaicGeometryOGC.fromJSON(geoJson)

  override def fromHEX(hex: String): MosaicGeometry = MosaicGeometryOGC.fromHEX(hex)
}
