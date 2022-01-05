package com.databricks.mosaic.core.geometry.multipolygon

import com.databricks.mosaic.core.geometry.point.MosaicPoint
import com.databricks.mosaic.core.geometry.polygon.MosaicPolygonOGC
import com.databricks.mosaic.core.geometry.{GeometryReader, MosaicGeometry, MosaicGeometryOGC}
import com.databricks.mosaic.core.types.model.GeometryTypeEnum.MULTIPOLYGON
import com.databricks.mosaic.core.types.model.{GeometryTypeEnum, InternalCoord, InternalGeometry}
import com.esri.core.geometry.Polygon
import com.esri.core.geometry.ogc.{OGCGeometry, OGCMultiPolygon}
import org.apache.spark.sql.catalyst.InternalRow

class MosaicMultiPolygonOGC(multiPolygon: OGCMultiPolygon)
  extends MosaicGeometryOGC(multiPolygon) with MosaicMultiPolygon {

  override def toInternal: InternalGeometry = {
    val n = multiPolygon.numGeometries()
    val polygons = for (i <- 0 until n)
      yield MosaicPolygonOGC(multiPolygon.geometryN(i)).toInternal
    val boundaries = polygons.map(_.boundaries.head).toArray
    val holes = polygons.flatMap(_.holes).toArray
    new InternalGeometry(MULTIPOLYGON.id, boundaries, holes)
  }

  override def getBoundary: Seq[MosaicPoint] = getBoundaryPoints

  override def getBoundaryPoints: Seq[MosaicPoint] = {
    val n = multiPolygon.numGeometries()
    val boundaries = for (i <- 0 until n)
      yield MosaicPolygonOGC(multiPolygon.geometryN(i)).getBoundaryPoints
    boundaries.reduce(_ ++ _)
  }

  override def getHoles: Seq[Seq[MosaicPoint]] = getHolePoints

  override def getHolePoints: Seq[Seq[MosaicPoint]] = {
    val n = multiPolygon.numGeometries()
    val holeGroups = for (i <- 0 until n)
      yield MosaicPolygonOGC(multiPolygon.geometryN(i)).getHolePoints
    holeGroups.reduce(_ ++ _)
  }

  override def getLength: Double = MosaicGeometryOGC(multiPolygon.boundary()).getLength

  override def flatten: Seq[MosaicGeometry] = asSeq

  override def asSeq: Seq[MosaicGeometry] = for (i <- 0 until multiPolygon.numGeometries()) yield MosaicGeometryOGC(multiPolygon.geometryN(i))
}

object MosaicMultiPolygonOGC extends GeometryReader {

  override def fromInternal(row: InternalRow): MosaicGeometry = {
    val internalGeom = InternalGeometry(row)
    val polygon = createPolygon(internalGeom.boundaries, internalGeom.holes)
    val ogcMultiLineString = new OGCMultiPolygon(polygon, MosaicGeometryOGC.spatialReference)
    MosaicMultiPolygonOGC(ogcMultiLineString)
  }

  //noinspection ZeroIndexToHead
  def createPolygon(shellCollection: Array[Array[InternalCoord]], holesCollection: Array[Array[Array[InternalCoord]]]): Polygon = {
    def addPath(polygon: Polygon, path: Array[InternalCoord]): Unit = {
      if (path.nonEmpty) {
        val start = path.head
        val middle = path.tail.dropRight(1)
        val last = path.last

        polygon.startPath(start.coords(0), start.coords(1))
        for (point <- middle) polygon.lineTo(point.coords(0), point.coords(1))
        if (!last.equals(start)) polygon.lineTo(last.coords(0), last.coords(1))
      }
    }

    val polygon = new Polygon()
    for (shell <- shellCollection) addPath(polygon, shell)
    for (holes <- holesCollection)
      for (hole <- holes) addPath(polygon, hole)

    polygon
  }

  def apply(multiPolygon: OGCGeometry): MosaicMultiPolygonOGC =
    new MosaicMultiPolygonOGC(multiPolygon.asInstanceOf[OGCMultiPolygon])

  override def fromPoints(points: Seq[MosaicPoint], geomType: GeometryTypeEnum.Value): MosaicGeometry = {
    throw new UnsupportedOperationException("fromPoints is not intended for creating MultiPolygons")
  }

  override def fromWKB(wkb: Array[Byte]): MosaicGeometry = MosaicGeometryOGC.fromWKB(wkb)

  override def fromWKT(wkt: String): MosaicGeometry = MosaicGeometryOGC.fromWKT(wkt)

  override def fromJSON(geoJson: String): MosaicGeometry = MosaicGeometryOGC.fromJSON(geoJson)

  override def fromHEX(hex: String): MosaicGeometry = MosaicGeometryOGC.fromHEX(hex)

}
