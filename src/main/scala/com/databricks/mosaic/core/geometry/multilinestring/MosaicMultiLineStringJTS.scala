package com.databricks.mosaic.core.geometry.multilinestring

import com.databricks.mosaic.core.geometry.linestring.{MosaicLineString, MosaicLineStringJTS}
import com.databricks.mosaic.core.geometry.point.MosaicPoint
import com.databricks.mosaic.core.geometry.{GeometryReader, MosaicGeometry, MosaicGeometryJTS}
import com.databricks.mosaic.core.types.model.GeometryTypeEnum.MULTILINESTRING
import com.databricks.mosaic.core.types.model.{GeometryTypeEnum, InternalCoord, InternalGeometry}
import com.esotericsoftware.kryo.io.Input
import org.apache.spark.sql.catalyst.InternalRow
import org.locationtech.jts.geom.{Geometry, GeometryFactory, LineString, MultiLineString}

class MosaicMultiLineStringJTS(multiLineString: MultiLineString)
  extends MosaicGeometryJTS(multiLineString) with MosaicMultiLineString {

  override def getHolePoints: Seq[Seq[Seq[MosaicPoint]]] = Nil

  override def getBoundaryPoints: Seq[Seq[MosaicPoint]] = {
    for (i <- 0 until multiLineString.getNumGeometries)
      yield {
        val lineString = multiLineString.getGeometryN(i).asInstanceOf[LineString]
        MosaicLineStringJTS.getPoints(lineString)
      }
  }

  override def toInternal: InternalGeometry = {
    val shells = for (i <- 0 until multiLineString.getNumGeometries)
      yield {
        val lineString = multiLineString.getGeometryN(i).asInstanceOf[LineString]
        lineString.getCoordinates.map(InternalCoord(_))
      }
    new InternalGeometry(MULTILINESTRING.id, shells.toArray, Array(Array(Array())))
  }

  override def getBoundary: Seq[MosaicPoint] = MosaicGeometryJTS(multiLineString.getBoundary).getBoundary

  override def getHoles: Seq[Seq[MosaicPoint]] = Nil

  override def flatten: Seq[MosaicGeometry] = asSeq

  override def asSeq: Seq[MosaicLineString] = for (i <- 0 until multiLineString.getNumGeometries)
    yield new MosaicLineStringJTS(multiLineString.getGeometryN(i).asInstanceOf[LineString])
}

object MosaicMultiLineStringJTS extends GeometryReader {

  override def fromInternal(row: InternalRow): MosaicGeometry = {
    val internalGeom = InternalGeometry(row)
    val gf = new GeometryFactory()
    val lineStrings = for (shell <- internalGeom.boundaries)
      yield gf.createLineString(shell.map(_.toCoordinate))
    MosaicMultiLineStringJTS(gf.createMultiLineString(lineStrings))
  }

  def apply(geometry: Geometry): MosaicMultiLineStringJTS = {
    new MosaicMultiLineStringJTS(geometry.asInstanceOf[MultiLineString])
  }

  override def fromPoints(points: Seq[MosaicPoint], geomType: GeometryTypeEnum.Value = MULTILINESTRING): MosaicGeometry = {
    throw new UnsupportedOperationException("fromPoints is not intended for creating MultiLineStrings")
  }

  override def fromWKB(wkb: Array[Byte]): MosaicGeometry = MosaicGeometryJTS.fromWKB(wkb)

  override def fromWKT(wkt: String): MosaicGeometry = MosaicGeometryJTS.fromWKT(wkt)

  override def fromJSON(geoJson: String): MosaicGeometry = MosaicGeometryJTS.fromJSON(geoJson)

  override def fromHEX(hex: String): MosaicGeometry = MosaicGeometryJTS.fromHEX(hex)

  override def fromKryo(row: InternalRow): MosaicGeometry = {
    val kryoBytes = row.getBinary(1)
    val input = new Input(kryoBytes)
    MosaicGeometryJTS.kryo.readObject(input, classOf[MosaicMultiLineStringJTS])
  }

}