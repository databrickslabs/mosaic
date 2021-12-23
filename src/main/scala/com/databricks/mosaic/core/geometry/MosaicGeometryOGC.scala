package com.databricks.mosaic.core.geometry

import com.esri.core.geometry.ogc.{OGCGeometry, OGCLinearRing}

case class MosaicGeometryOGC(geom: OGCGeometry)
  extends MosaicGeometry {

  override def toWKB: Array[Byte] = geom.asBinary().array()

  override def getAPI: String = "OGC"

  override def getCentroid: MosaicPoint = MosaicPointOGC(geom.centroid())

  override def getCoordinates: Seq[MosaicPoint] = geom.geometryType() match {
    case "Polygon" => MosaicPolygonOGC(geom).getBoundaryPoints
    case "MultiPolygon" => MosaicMultiPolygonOGC(geom).getBoundaryPoints
    case _ => throw new NotImplementedError("Geometry type not implemented yet!")
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
    //required to use object equals to perform exact equals
    //noinspection ComparingUnrelatedTypes
    this.geom.equals(otherGeom.asInstanceOf[Object])
  }

}
