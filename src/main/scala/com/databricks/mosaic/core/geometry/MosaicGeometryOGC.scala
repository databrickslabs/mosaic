package com.databricks.mosaic.core.geometry

import com.databricks.mosaic.core.geometry.linestring.MosaicLineStringOGC
import com.databricks.mosaic.core.geometry.multilinestring.MosaicMultiLineStringOGC
import com.databricks.mosaic.core.geometry.multipoint.MosaicMultiPointOGC
import com.databricks.mosaic.core.geometry.multipolygon.MosaicMultiPolygonOGC
import com.databricks.mosaic.core.geometry.point.{MosaicPoint, MosaicPointOGC}
import com.databricks.mosaic.core.geometry.polygon.MosaicPolygonOGC
import com.databricks.mosaic.core.types.model.GeometryTypeEnum._
import com.databricks.mosaic.core.types.model.{GeometryTypeEnum, InternalGeometry}
import com.esri.core.geometry.SpatialReference
import com.esri.core.geometry.ogc._
import org.apache.spark.sql.catalyst.InternalRow
import org.locationtech.jts.io.{WKBReader, WKBWriter}

import java.nio.ByteBuffer

abstract class MosaicGeometryOGC(geom: OGCGeometry)
  extends MosaicGeometry {

  override def getAPI: String = "OGC"

  override def getCentroid: MosaicPoint = MosaicPointOGC(geom.centroid())

  override def isEmpty: Boolean = geom.isEmpty

  override def buffer(distance: Double): MosaicGeometry = MosaicGeometryOGC(geom.buffer(distance))

  override def simplify(tolerance: Double): MosaicGeometry = MosaicGeometryOGC(geom.makeSimple())

  override def intersection(other: MosaicGeometry): MosaicGeometry = {
    val otherGeom = other.asInstanceOf[MosaicGeometryOGC].getGeom
    MosaicGeometryOGC(this.getGeom.intersection(otherGeom))
  }

  override def contains(other:MosaicGeometry): Boolean = {
    val otherGeom = other.asInstanceOf[MosaicGeometryOGC].getGeom
    this.getGeom.contains(otherGeom)
  }

  /**
   * The naming convention in ESRI bindings is different.
   * isSimple actually reflects validity of a geometry.
   *
   * @see [[OGCGeometry]] for isSimple documentation.
   * @return A boolean flag indicating validity.
   */
  override def isValid: Boolean = geom.isSimple

  override def getGeometryType: String = geom.geometryType()

  override def getArea: Double = geom.getEsriGeometry.calculateArea2D()

  override def equals(other: MosaicGeometry): Boolean = {
    val otherGeom = other.asInstanceOf[MosaicGeometryOGC].getGeom
    // required to use object equals to perform exact equals
    //noinspection ComparingUnrelatedTypes
    this.getGeom.equals(otherGeom.asInstanceOf[Object])
  }

  def getGeom: OGCGeometry = geom

  override def equals(other: java.lang.Object): Boolean = false

  override def hashCode: Int = geom.hashCode()

  override def boundary: MosaicGeometry = MosaicGeometryOGC(geom.boundary())

  override def distance(geom2: MosaicGeometry): Double = this.getGeom.distance(geom2.asInstanceOf[MosaicGeometryOGC].getGeom)

  override def toWKB: Array[Byte] = geom.asBinary().array()

  override def toWKT: String = geom.asText()

  override def toJSON: String = geom.asGeoJson()

  override def toHEX: String = WKBWriter.toHex(geom.asBinary().array())

}

object MosaicGeometryOGC extends GeometryReader {

  val spatialReference: SpatialReference = SpatialReference.create(4326)

  override def fromWKT(wkt: String): MosaicGeometry = MosaicGeometryOGC(OGCGeometry.fromText(wkt))

  def apply(geom: OGCGeometry): MosaicGeometryOGC = GeometryTypeEnum.fromString(geom.geometryType()) match {
    case POINT => MosaicPointOGC(geom)
    case MULTIPOINT => MosaicMultiPointOGC(geom)
    case POLYGON => MosaicPolygonOGC(geom)
    case MULTIPOLYGON => MosaicMultiPolygonOGC(geom)
    case LINESTRING => MosaicLineStringOGC(geom)
    case MULTILINESTRING => MosaicMultiLineStringOGC(geom)
  }

  override def fromHEX(hex: String): MosaicGeometry = {
    val bytes = WKBReader.hexToBytes(hex)
    fromWKB(bytes)
  }

  override def fromWKB(wkb: Array[Byte]): MosaicGeometryOGC = MosaicGeometryOGC(OGCGeometry.fromBinary(ByteBuffer.wrap(wkb)))

  override def fromJSON(geoJson: String): MosaicGeometry = MosaicGeometryOGC(OGCGeometry.fromGeoJson(geoJson))

  override def fromPoints(points: Seq[MosaicPoint], geomType: GeometryTypeEnum.Value): MosaicGeometry = {
    reader(geomType.id).fromPoints(points, geomType)
  }

  def reader(geomTypeId: Int): GeometryReader = GeometryTypeEnum.fromId(geomTypeId) match {
    case POINT => MosaicPointOGC
    case MULTIPOINT => MosaicMultiPointOGC
    case POLYGON => MosaicPolygonOGC
    case MULTIPOLYGON => MosaicMultiPolygonOGC
    case LINESTRING => MosaicLineStringOGC
    case MULTILINESTRING => MosaicMultiLineStringOGC
  }

  override def fromInternal(row: InternalRow): MosaicGeometry = {
    val internalGeometry = InternalGeometry(row)
    reader(internalGeometry.typeId).fromInternal(row)
  }

}
