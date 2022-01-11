package com.databricks.mosaic.core.geometry

import com.databricks.mosaic.core.geometry.linestring.MosaicLineStringJTS
import com.databricks.mosaic.core.geometry.multilinestring.MosaicMultiLineStringJTS
import com.databricks.mosaic.core.geometry.multipoint.MosaicMultiPointJTS
import com.databricks.mosaic.core.geometry.multipolygon.MosaicMultiPolygonJTS
import com.databricks.mosaic.core.geometry.point.{MosaicPoint, MosaicPointJTS}
import com.databricks.mosaic.core.geometry.polygon.MosaicPolygonJTS
import com.databricks.mosaic.core.types.model.GeometryTypeEnum._
import com.databricks.mosaic.core.types.model.{GeometryTypeEnum, InternalGeometry}
import org.apache.spark.sql.catalyst.InternalRow
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.io.geojson.{GeoJsonReader, GeoJsonWriter}
import org.locationtech.jts.io.{WKBReader, WKBWriter, WKTReader, WKTWriter}
import org.locationtech.jts.simplify.DouglasPeuckerSimplifier

abstract class MosaicGeometryJTS(geom: Geometry)
  extends MosaicGeometry {

  override def getAPI: String = "JTS"

  override def getCentroid: MosaicPoint = new MosaicPointJTS(geom.getCentroid)

  override def isEmpty: Boolean = geom.isEmpty

  override def boundary: MosaicGeometry = MosaicGeometryJTS(geom.getBoundary)

  override def buffer(distance: Double): MosaicGeometry = MosaicGeometryJTS(geom.buffer(distance))

  override def simplify(tolerance: Double = 1e-8): MosaicGeometry = MosaicGeometryJTS(
    DouglasPeuckerSimplifier.simplify(geom, tolerance)
  )

  override def intersection(other: MosaicGeometry): MosaicGeometry = {
    val otherGeom = other.asInstanceOf[MosaicGeometryJTS].getGeom
    val intersection = this.geom.intersection(otherGeom)
    MosaicGeometryJTS(intersection)
  }

  override def contains(other: MosaicGeometry): Boolean = {
    val otherGeom = other.asInstanceOf[MosaicGeometryJTS].getGeom
    this.geom.contains(otherGeom)
  }

  def getGeom: Geometry = geom

  override def isValid: Boolean = geom.isValid

  override def getGeometryType: String = geom.getGeometryType

  override def getArea: Double = geom.getArea

  override def equals(other: MosaicGeometry): Boolean = {
    val otherGeom = other.asInstanceOf[MosaicGeometryJTS].getGeom
    this.geom.equalsExact(otherGeom)
  }

  override def equals(other: java.lang.Object): Boolean = false

  override def hashCode: Int = geom.hashCode()

  override def getLength: Double = geom.getLength

  override def distance(geom2: MosaicGeometry): Double = getGeom.distance(geom2.asInstanceOf[MosaicGeometryJTS].getGeom)

  override def toWKT: String = new WKTWriter().write(geom)

  override def toJSON: String = new GeoJsonWriter().write(geom)

  override def toHEX: String = WKBWriter.toHex(toWKB)

  override def toWKB: Array[Byte] = new WKBWriter().write(geom)

}

object MosaicGeometryJTS extends GeometryReader {

  override def fromWKT(wkt: String): MosaicGeometry = MosaicGeometryJTS(new WKTReader().read(wkt))

  override def fromHEX(hex: String): MosaicGeometry = {
    val bytes = WKBReader.hexToBytes(hex)
    fromWKB(bytes)
  }

  override def fromWKB(wkb: Array[Byte]): MosaicGeometry = MosaicGeometryJTS(new WKBReader().read(wkb))

  def apply(geom: Geometry): MosaicGeometryJTS = GeometryTypeEnum.fromString(geom.getGeometryType) match {
    case POINT => MosaicPointJTS(geom)
    case MULTIPOINT => MosaicMultiPointJTS(geom)
    case POLYGON => MosaicPolygonJTS(geom)
    case MULTIPOLYGON => MosaicMultiPolygonJTS(geom)
    case LINESTRING => MosaicLineStringJTS(geom)
    case MULTILINESTRING => MosaicMultiLineStringJTS(geom)
    case LINEARRING => MosaicLineStringJTS(geom)
  }

  override def fromJSON(geoJson: String): MosaicGeometry = MosaicGeometryJTS(new GeoJsonReader().read(geoJson))

  override def fromPoints(points: Seq[MosaicPoint], geomType: GeometryTypeEnum.Value): MosaicGeometry = {
    reader(geomType.id).fromPoints(points, geomType)
  }

  def reader(geomTypeId: Int): GeometryReader = GeometryTypeEnum.fromId(geomTypeId) match {
    case POINT => MosaicPointJTS
    case MULTIPOINT => MosaicMultiPointJTS
    case POLYGON => MosaicPolygonJTS
    case MULTIPOLYGON => MosaicMultiPolygonJTS
    case LINESTRING => MosaicLineStringJTS
    case MULTILINESTRING => MosaicMultiLineStringJTS
  }

  override def fromInternal(row: InternalRow): MosaicGeometry = {
    val internalGeometry = InternalGeometry(row)
    reader(internalGeometry.typeId).fromInternal(row)
  }

}