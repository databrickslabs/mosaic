package com.databricks.mosaic.core.geometry

import java.nio.ByteBuffer

import com.esri.core.geometry.{Polygon, SpatialReference}
import com.esri.core.geometry.ogc.{OGCGeometry, OGCLinearRing, OGCPolygon}

import com.databricks.mosaic.expressions.format.Conversions

case class MosaicGeometryOGC(geom: OGCGeometry)
  extends MosaicGeometry {

  override def toWKB: Array[Byte] = geom.asBinary().array()

  override def getAPI: String = "OGC"

  override def getCentroid: MosaicPoint = MosaicPointOGC(geom.centroid())

  override def getCoordinates: Seq[MosaicPoint] = geom.geometryType() match {
    case "Polygon" => MosaicPolygonOGC(geom).getBoundaryPoints
    case "MultiPolygon" => MosaicMultiPolygonOGC(geom).getBoundaryPoints
    case _ => throw new NotImplementedError("Geometry type not implemented yet!") // scalastyle:ignore
  }

  override def isEmpty: Boolean = geom.isEmpty

  override def getBoundary: Seq[MosaicPoint] =
    geom.geometryType() match {
      case "LinearRing" => MosaicPolygonOGC.getPoints(geom.asInstanceOf[OGCLinearRing])
      case "Polygon" => MosaicPolygonOGC(geom).getBoundaryPoints
      case "MultiPolygon" => MosaicMultiPolygonOGC(geom).getBoundaryPoints
    }

  override def getHoles: Seq[Seq[MosaicPoint]] =
    geom.geometryType() match {
      case "LinearRing" => Seq(MosaicPolygonOGC.getPoints(geom.asInstanceOf[OGCLinearRing]))
      case "Polygon" => MosaicPolygonOGC(geom).getHolePoints
      case "MultiPolygon" => MosaicMultiPolygonOGC(geom).getHolePoints
    }

  override def boundary: MosaicGeometry = MosaicGeometryOGC(geom.boundary())

  override def buffer(distance: Double): MosaicGeometry = MosaicGeometryOGC(geom.buffer(distance))

  override def simplify(tolerance: Double): MosaicGeometry = MosaicGeometryOGC(geom.makeSimple())

  override def intersection(other: MosaicGeometry): MosaicGeometry = {
    val otherGeom = other.asInstanceOf[MosaicGeometryOGC].geom
    MosaicGeometryOGC(this.geom.intersection(otherGeom))
  }

  override def equals(other: MosaicGeometry): Boolean = {
    val otherGeom = other.asInstanceOf[MosaicGeometryOGC].geom
    // required to use object equals to perform exact equals
    //noinspection ComparingUnrelatedTypes
    this.geom.equals(otherGeom.asInstanceOf[Object])
  }

  override def equals(other: java.lang.Object): Boolean = false

  override def hashCode: Int = geom.hashCode()

}

object MosaicGeometryOGC extends GeometryReaders {

  override def fromWKB(wkb: Array[Byte]): MosaicGeometryOGC = MosaicGeometryOGC(OGCGeometry.fromBinary(ByteBuffer.wrap(wkb)))

  override def fromWKT(wkt: String): MosaicGeometry = MosaicGeometryOGC(OGCGeometry.fromText(wkt))

  override def fromHEX(hex: String): MosaicGeometry = {
    val wkb = Conversions.Typed.hex2wkb(hex)
    fromWKB(wkb)
  }

  override def fromJSON(geoJson: String): MosaicGeometry = MosaicGeometryOGC(OGCGeometry.fromGeoJson(geoJson))

  override def fromPoints(points: Seq[MosaicPoint]): MosaicGeometry = {
    val polygon = new Polygon()
    val start = points.head
    polygon.startPath(start.getX, start.getY)
    for (i <- 1 until points.size)
      yield polygon.lineTo(points(i).getX, points(i).getY)
    MosaicGeometryOGC(new OGCPolygon(polygon, SpatialReference.create(4326)))
  }
}
