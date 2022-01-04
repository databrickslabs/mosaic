package com.databricks.mosaic.core.geometry

import com.databricks.mosaic.core.geometry.multipolygon.MosaicMultiPolygonJTS
import com.databricks.mosaic.core.geometry.point.{MosaicPoint, MosaicPointJTS}
import com.databricks.mosaic.core.geometry.polygon.MosaicPolygonJTS
import com.databricks.mosaic.core.types.model.{InternalCoord, InternalGeometry}
import org.apache.spark.sql.catalyst.InternalRow
import org.locationtech.jts.geom.{Geometry, GeometryFactory, LinearRing, Polygon}
import org.locationtech.jts.io.geojson.{GeoJsonReader, GeoJsonWriter}
import org.locationtech.jts.io.{WKBReader, WKBWriter, WKTReader, WKTWriter}
import org.locationtech.jts.simplify.DouglasPeuckerSimplifier

case class MosaicGeometryJTS(geom: Geometry)
  extends MosaicGeometry {

  override def getAPI: String = "JTS"

  override def getCentroid: MosaicPoint = MosaicPointJTS(geom.getCentroid)

  override def getCoordinates: Seq[MosaicPoint] = {
    geom.getCoordinates.map(MosaicPointJTS(_))
  }

  override def isEmpty: Boolean = geom.isEmpty

  override def getBoundary: Seq[MosaicPoint] = {
    geom.getGeometryType match {
      case "LinearRing" => MosaicPolygonJTS.getPoints(geom.asInstanceOf[LinearRing])
      case "Polygon" => MosaicPolygonJTS(geom).getBoundaryPoints
      case "MultiPolygon" => MosaicMultiPolygonJTS(geom).getBoundaryPoints
    }
  }


  override def getHoles: Seq[Seq[MosaicPoint]] = {
    geom.getGeometryType match {
      case "LinearRing" => Seq(MosaicPolygonJTS.getPoints(geom.asInstanceOf[LinearRing]))
      case "Polygon" => MosaicPolygonJTS(geom).getHolePoints
      case "MultiPolygon" => MosaicMultiPolygonJTS(geom).getHolePoints
    }
  }

  /**
   * Flattens this geometry instance.
   * This method assumes only a single level of nesting.
   * @return A collection of piece-wise geometries.
   */
  override def flatten: Seq[MosaicGeometry] = {
    geom.getGeometryType match {
      case "Polygon" => List(this)
      case "MultiPolygon" => for (
        i <- 0 until geom.getNumGeometries
      ) yield MosaicGeometryJTS(geom.getGeometryN(i))
    }
  }

  override def boundary: MosaicGeometry = MosaicGeometryJTS(geom.getBoundary)

  override def buffer(distance: Double): MosaicGeometry = MosaicGeometryJTS(geom.buffer(distance))

  override def simplify(tolerance: Double = 1e-8): MosaicGeometry = MosaicGeometryJTS(
    DouglasPeuckerSimplifier.simplify(geom, tolerance)
  )

  override def intersection(other: MosaicGeometry): MosaicGeometry = {
    val otherGeom = other.asInstanceOf[MosaicGeometryJTS].geom
    val intersection = this.geom.intersection(otherGeom)
    MosaicGeometryJTS(intersection)
  }

  override def isValid: Boolean = geom.isValid

  override def getGeometryType: String = geom.getGeometryType

  override def getArea: Double = geom.getArea

  override def equals(other: MosaicGeometry): Boolean = {
    val otherGeom = other.asInstanceOf[MosaicGeometryJTS].geom
    this.geom.equalsExact(otherGeom)
  }

  /**
   * Converts a Polygon to an instance of [[InternalGeometry]].
   * @param g An instance of Polygon to be converted.
   * @param typeName Type name to used to construct the instance
   *                 of [[InternalGeometry]].
   * @return An instance of [[InternalGeometry]].
   */
  private def fromPolygon(g: Polygon, typeName: String): InternalGeometry = {
    val boundary = g.getBoundary
    val shell = boundary.getGeometryN(0).getCoordinates.map(InternalCoord(_))
    val holes = for (i <- 1 until boundary.getNumGeometries) yield boundary.getGeometryN(i).getCoordinates.map(InternalCoord(_))
    new InternalGeometry(typeName, Array(shell), Array(holes.toArray))
  }

  override def toInternal: InternalGeometry = {
    geom.getGeometryType match {
      case "Polygon" =>
        fromPolygon(geom.asInstanceOf[Polygon], "Polygon")
      case "MultiPolygon" =>
        val geoms = for (i <- 0 until geom.getNumGeometries) yield fromPolygon(geom.getGeometryN(i).asInstanceOf[Polygon], "MultiPolygon")
        geoms.reduce(_ merge _)
    }
  }

  override def toWKB: Array[Byte] = new WKBWriter().write(geom)

  override def toWKT: String = new WKTWriter().write(geom)

  override def toJSON: String = new GeoJsonWriter().write(geom)

  override def toHEX: String = WKBWriter.toHex(toWKB)

}

object MosaicGeometryJTS extends GeometryReader {

  override def fromWKB(wkb: Array[Byte]): MosaicGeometry = MosaicGeometryJTS(new WKBReader().read(wkb))

  override def fromWKT(wkt: String): MosaicGeometry = MosaicGeometryJTS(new WKTReader().read(wkt))

  override def fromHEX(hex: String): MosaicGeometry = {
    val bytes = WKBReader.hexToBytes(hex)
    fromWKB(bytes)
  }

  override def fromJSON(geoJson: String): MosaicGeometry = MosaicGeometryJTS(new GeoJsonReader().read(geoJson))

  def fromPoints(points: Seq[MosaicPoint]): MosaicGeometryJTS = {
    val gf = new GeometryFactory()
    val polygon = gf.createPolygon(points.map(_.coord).toArray)
    new MosaicGeometryJTS(polygon)
  }

  override def fromInternal(row: InternalRow): MosaicGeometry = {
    val gf = new GeometryFactory()
    val internalGeom = InternalGeometry(row)
    val shellCollection = internalGeom.boundaries.map(ring => ring.map(_.toCoordinate)).map(gf.createLinearRing)
    val holesCollection = internalGeom.holes.map(
      holes => holes.map(ring => ring.map(_.toCoordinate)).map(gf.createLinearRing)
    )
    val geometry = internalGeom.typeName match {
      case "Polygon" => gf.createPolygon(shellCollection.head, holesCollection.head)
      case "MultiPolygon" =>
        val polygons = shellCollection.zip(holesCollection).map{ case (shell, holes) => gf.createPolygon(shell, holes)}
        gf.createMultiPolygon(polygons)
      case "LinearRing" => shellCollection.head
      case _ => throw new NotImplementedError("Geometry type not implemented yet.")
    }
    MosaicGeometryJTS(geometry)
  }
}