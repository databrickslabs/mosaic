package com.databricks.mosaic.core.geometry.multipolygon

import com.databricks.mosaic.core.geometry.multipoint.MosaicMultiPointJTS
import com.databricks.mosaic.core.geometry.point.MosaicPoint
import com.databricks.mosaic.core.geometry.polygon.MosaicPolygonJTS
import com.databricks.mosaic.core.geometry.{GeometryReader, MosaicGeometry, MosaicGeometryJTS}
import com.databricks.mosaic.core.types.model.GeometryTypeEnum.MULTIPOLYGON
import com.databricks.mosaic.core.types.model.{GeometryTypeEnum, InternalGeometry}
import com.esotericsoftware.kryo.io.Input
import org.apache.spark.sql.catalyst.InternalRow
import org.locationtech.jts.geom.{Geometry, GeometryFactory, MultiPolygon, Polygon}

class MosaicMultiPolygonJTS(multiPolygon: MultiPolygon)
  extends MosaicGeometryJTS(multiPolygon) with MosaicMultiPolygon {

  override def toInternal: InternalGeometry = {
    val n = multiPolygon.getNumGeometries
    val polygons = for (i <- 0 until n)
      yield MosaicPolygonJTS(multiPolygon.getGeometryN(i)).toInternal
    val boundaries = polygons.map(_.boundaries.head).toArray
    val holes = polygons.flatMap(_.holes).toArray
    new InternalGeometry(MULTIPOLYGON.id, boundaries, holes)
  }

  override def getBoundary: Seq[MosaicPoint] = getBoundaryPoints

  override def getBoundaryPoints: Seq[MosaicPoint] = {
    val n = multiPolygon.getNumGeometries
    val boundaries = for (i <- 0 until n)
      yield {
        val polygon = MosaicPolygonJTS(multiPolygon.getGeometryN(i).asInstanceOf[Polygon])
        polygon.getBoundaryPoints
      }
    boundaries.reduce(_ ++ _)
  }

  override def getHoles: Seq[Seq[MosaicPoint]] = getHolePoints

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

  override def flatten: Seq[MosaicGeometry] = asSeq

  override def asSeq: Seq[MosaicGeometry] = for (i <- 0 until multiPolygon.getNumGeometries) yield MosaicGeometryJTS(multiPolygon.getGeometryN(i))
}

object MosaicMultiPolygonJTS extends GeometryReader {

  override def fromInternal(row: InternalRow): MosaicGeometry = {
    val gf = new GeometryFactory()
    val internalGeom = InternalGeometry(row)

    gf.createLinearRing(gf.createLineString().getCoordinates)
    val polygons = internalGeom.boundaries.zip(internalGeom.holes).map {
      case (boundaryRing, holesRings) =>
        val shell = gf.createLinearRing(boundaryRing.map(_.toCoordinate))
        val holes = holesRings.map(ring => ring.map(_.toCoordinate)).map(gf.createLinearRing)
        gf.createPolygon(shell, holes)
    }
    val multiPolygon = gf.createMultiPolygon(polygons)
    MosaicMultiPolygonJTS(multiPolygon)
  }

  def apply(multiPolygon: Geometry): MosaicMultiPolygonJTS =
    new MosaicMultiPolygonJTS(multiPolygon.asInstanceOf[MultiPolygon])

  override def fromPoints(points: Seq[MosaicPoint], geomType: GeometryTypeEnum.Value): MosaicGeometry = {
    throw new UnsupportedOperationException("fromPoints is not intended for creating MultiPolygons")
  }

  override def fromWKB(wkb: Array[Byte]): MosaicGeometry = MosaicGeometryJTS.fromWKB(wkb)

  override def fromWKT(wkt: String): MosaicGeometry = MosaicGeometryJTS.fromWKT(wkt)

  override def fromJSON(geoJson: String): MosaicGeometry = MosaicGeometryJTS.fromJSON(geoJson)

  override def fromHEX(hex: String): MosaicGeometry = MosaicGeometryJTS.fromHEX(hex)

  override def fromKryo(row: InternalRow): MosaicGeometry = {
    val kryoBytes = row.getBinary(1)
    val input = new Input(kryoBytes)
    MosaicGeometryJTS.kryo.readObject(input, classOf[MosaicMultiPolygonJTS])
  }

}
