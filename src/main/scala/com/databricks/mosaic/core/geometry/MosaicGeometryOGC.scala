package com.databricks.mosaic.core.geometry

import com.databricks.mosaic.core.geometry.multipolygon.MosaicMultiPolygonOGC
import com.databricks.mosaic.core.geometry.point.{MosaicPoint, MosaicPointOGC}
import com.databricks.mosaic.core.geometry.polygon.MosaicPolygonOGC
import com.databricks.mosaic.core.types.model.{InternalCoord, InternalGeometry}
import com.esri.core.geometry.ogc._
import com.esri.core.geometry.{Polygon, SpatialReference}
import org.apache.spark.sql.catalyst.InternalRow
import org.locationtech.jts.io.{WKBReader, WKBWriter}

import java.nio.ByteBuffer
import scala.util.{Success, Try}

case class MosaicGeometryOGC(geom: OGCGeometry)
  extends MosaicGeometry {

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

  override def flatten: Seq[MosaicGeometry] = {
    geom.geometryType() match {
      case "Polygon" => List(this)
      case "MultiPolygon" =>
        val multiPolygon = geom.asInstanceOf[OGCMultiPolygon]
        for (
        i <- 0 until multiPolygon.numGeometries()
      ) yield MosaicGeometryOGC(multiPolygon.geometryN(i))
    }
  }

  override def boundary: MosaicGeometry = MosaicGeometryOGC(geom.boundary())

  override def buffer(distance: Double): MosaicGeometry = MosaicGeometryOGC(geom.buffer(distance))

  override def simplify(tolerance: Double): MosaicGeometry = MosaicGeometryOGC(geom.makeSimple())

  override def intersection(other: MosaicGeometry): MosaicGeometry = {
    val otherGeom = other.asInstanceOf[MosaicGeometryOGC].geom
    MosaicGeometryOGC(this.geom.intersection(otherGeom))
  }

  /**
   * The naming convention in ESRI bindings is different.
   * isSimple actually reflects validity of a geometry.
   * @see [[OGCGeometry]] for isSimple documentation.
   * @return A boolean flag indicating validity.
   */
  override def isValid: Boolean =  geom.isSimple

  override def getGeometryType: String = geom.geometryType()

  override def getArea: Double = geom.getEsriGeometry.calculateArea2D()


  override def equals(other: MosaicGeometry): Boolean = {
    val otherGeom = other.asInstanceOf[MosaicGeometryOGC].geom
    //required to use object equals to perform exact equals
    //noinspection ComparingUnrelatedTypes
    this.geom.equals(otherGeom.asInstanceOf[Object])
  }

  /**
   * Converts a Polygon to an instance of [[InternalGeometry]].
   * @param g An instance of Polygon to be converted.
   * @param typeName Type name to used to construct the instance
   *                 of [[InternalGeometry]].
   * @return An instance of [[InternalGeometry]].
   */
  private def fromPolygon(g: OGCPolygon, typeName: String): InternalGeometry = {
    def ringToInternalCoords(ring: OGCLineString): Array[InternalCoord] = {
      for (i <- 0 until ring.numPoints())
        yield InternalCoord(MosaicPointOGC(ring.pointN(i)).coord)
    }.toArray

    val boundary = g.boundary().geometryN(0).asInstanceOf[OGCLineString]
    val shell = ringToInternalCoords(boundary)
    val holes = for (i <- 0 until g.numInteriorRing())
      yield ringToInternalCoords(g.interiorRingN(i))

    new InternalGeometry(typeName, Array(shell), Array(holes.toArray))
  }

  override def toInternal: InternalGeometry = {
    geom.geometryType() match {
      case "Polygon" =>
        fromPolygon(geom.asInstanceOf[OGCPolygon], "Polygon")
      case "MultiPolygon" =>
        val geoms = for (i <- 0 until geom.asInstanceOf[OGCMultiPolygon].numGeometries())
          yield fromPolygon(geom.asInstanceOf[OGCMultiPolygon].geometryN(i).asInstanceOf[OGCPolygon], "MultiPolygon")
        geoms.reduce(_ merge _)
    }
  }

  override def toWKB: Array[Byte] =  geom.asBinary().array()

  override def toWKT: String = geom.asText()

  override def toJSON: String = geom.asGeoJson()

  override def toHEX: String = WKBWriter.toHex(geom.asBinary().array())

}

object MosaicGeometryOGC extends GeometryReader {

  private val spatialReference = SpatialReference.create(4326)

  override def fromWKB(wkb: Array[Byte]): MosaicGeometryOGC = MosaicGeometryOGC(OGCGeometry.fromBinary(ByteBuffer.wrap(wkb)))

  override def fromWKT(wkt: String): MosaicGeometry = MosaicGeometryOGC(OGCGeometry.fromText(wkt))

  override def fromHEX(hex: String): MosaicGeometry = {
    val bytes = WKBReader.hexToBytes(hex)
    fromWKB(bytes)
  }

  override def fromJSON(geoJson: String): MosaicGeometry = {
    val x = Try(MosaicGeometryOGC(OGCGeometry.fromGeoJson(geoJson)))
    x match {
      case Success(result) => result
      case _ => null
    }
  }

  override def fromPoints(points: Seq[MosaicPoint]): MosaicGeometry = {
    val polygon = new Polygon()
    val start = points.head
    polygon.startPath(start.getX, start.getY)
    for (i <- 1 until points.size)
      yield polygon.lineTo(points(i).getX, points(i).getY)
    MosaicGeometryOGC(new OGCPolygon(polygon, spatialReference))
  }

  //noinspection ZeroIndexToHead
  private def createPolygon(shellCollection: Array[Array[InternalCoord]], holesCollection: Array[Array[Array[InternalCoord]]]) = {
    def addPath(polygon: Polygon, path: Array[InternalCoord]): Unit = {
      val start = path.head
      val middle = path.tail.dropRight(1)
      val last = path.last

      polygon.startPath(start.coords(0), start.coords(1))
      for (point <- middle) polygon.lineTo(point.coords(0), point.coords(1))
      if (!last.equals(start)) polygon.lineTo(last.coords(0), last.coords(1))
    }

    val polygon = new Polygon()
    for (shell <- shellCollection) addPath(polygon, shell)
    for (holes <- holesCollection)
      for (hole <- holes) addPath(polygon, hole)

    polygon
  }

  override def fromInternal(row: InternalRow): MosaicGeometry = {
    val internalGeometry = InternalGeometry(row)
    val polygon = createPolygon(internalGeometry.boundaries, internalGeometry.holes)
    internalGeometry.typeName match {
      case "Polygon" => MosaicGeometryOGC(new OGCPolygon(polygon, spatialReference))
      case "MultiPolygon" => MosaicGeometryOGC(new OGCMultiPolygon(polygon, spatialReference).convertToMulti())
    }
  }


}
