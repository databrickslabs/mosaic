package com.databricks.mosaic.core.geometry.linestring

import com.databricks.mosaic.core.geometry.multilinestring.MosaicMultiLineStringOGC
import com.databricks.mosaic.core.geometry.point.{MosaicPoint, MosaicPointOGC}
import com.databricks.mosaic.core.geometry.polygon.MosaicPolygonOGC
import com.databricks.mosaic.core.geometry.{GeometryReader, MosaicGeometry, MosaicGeometryOGC}
import com.databricks.mosaic.core.types.model.GeometryTypeEnum.LINESTRING
import com.databricks.mosaic.core.types.model.{GeometryTypeEnum, InternalCoord, InternalGeometry}
import com.esri.core.geometry.ogc.{OGCGeometry, OGCLineString}
import org.apache.spark.sql.catalyst.InternalRow

class MosaicLineStringOGC(lineString: OGCLineString)
  extends MosaicGeometryOGC(lineString) with MosaicLineString {

  override def asSeq: Seq[MosaicPoint] = getBoundaryPoints

  override def getBoundaryPoints: Seq[MosaicPoint] = {
    MosaicLineStringOGC.getPoints(lineString)
  }

  override def toInternal: InternalGeometry = {
    val shell = for (i <- 0 until lineString.numPoints())
      yield {
        val point = lineString.pointN(i)
        InternalCoord(MosaicPointOGC(point).coord)
      }
    new InternalGeometry(LINESTRING.id, Array(shell.toArray), Array(Array(Array())))
  }

  override def getBoundary: Seq[MosaicPoint] = getBoundaryPoints

  override def getHoles: Seq[Seq[MosaicPoint]] = getHolePoints

  override def getHolePoints: Seq[Seq[MosaicPoint]] = Nil

  override def getLength: Double = lineString.length()

  override def flatten: Seq[MosaicGeometry] = List(this)
}

object MosaicLineStringOGC extends GeometryReader {

  def getPoints(lineString: OGCLineString): Seq[MosaicPoint] = {
    for (i <- 0 until lineString.numPoints())
      yield MosaicPointOGC(lineString.pointN(i))
  }

  override def fromInternal(row: InternalRow): MosaicGeometry = {
    val internalGeom = InternalGeometry(row)
    val polyline = MosaicMultiLineStringOGC.createPolyline(internalGeom.boundaries)
    val ogcLineString = new OGCLineString(polyline, 0, MosaicGeometryOGC.spatialReference)
    MosaicLineStringOGC(ogcLineString)
  }

  def apply(ogcGeometry: OGCGeometry): MosaicLineStringOGC = {
    new MosaicLineStringOGC(ogcGeometry.asInstanceOf[OGCLineString])
  }

  override def fromPoints(points: Seq[MosaicPoint], geomType: GeometryTypeEnum.Value = LINESTRING): MosaicGeometry = {
    require(geomType.id == LINESTRING.id)
    val polygon = MosaicPolygonOGC.fromPoints(points).asInstanceOf[MosaicGeometryOGC].getGeom
    MosaicLineStringOGC(polygon.boundary().asInstanceOf[OGCLineString])
  }

  override def fromWKB(wkb: Array[Byte]): MosaicGeometry = MosaicGeometryOGC.fromWKB(wkb)

  override def fromWKT(wkt: String): MosaicGeometry = MosaicGeometryOGC.fromWKT(wkt)

  override def fromJSON(geoJson: String): MosaicGeometry = MosaicGeometryOGC.fromJSON(geoJson)

  override def fromHEX(hex: String): MosaicGeometry = MosaicGeometryOGC.fromHEX(hex)

}