package com.databricks.mosaic.core.types.model

import org.locationtech.jts.geom.{Geometry, GeometryFactory, LineString, MultiPoint, Point, Polygon}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import com.databricks.mosaic.core.types.InternalCoordType
import com.databricks.mosaic.core.types.model.GeometryTypeEnum._

/**
 * A case class modeling Polygons and MultiPolygons.
 * @param typeName "Polygon" or "MultiPolygon" indicating the type of the instance.
 * @param boundaries A collection of boundaries, for Polygon the length is 1, for
 *                   MultiPolygon the collection will contain boundaries of each
 *                   sub Polygon.
 * @param holes A collection of hole collections, for Polygon the length is 1, for
 *              MultiPolygon the collection will contain hole collection for each
 *              sub Polygon.
 */
case class InternalGeometry(
   typeId: Int,
   boundaries: Array[Array[InternalCoord]],
   holes: Array[Array[Array[InternalCoord]]]
) {

  /**
   * Convert to JTS [[Geometry]] instance.
   * @return An instance of [[Geometry]].
   */
  def toGeom: Geometry = {
    val gf = new GeometryFactory()
    def createPolygon(ind: Int) = {
      val boundaryRing = gf.createLinearRing(boundaries(ind).map(_.toCoordinate))
      val holesRings = holes(ind).map(hole => gf.createLinearRing(hole.map(_.toCoordinate)))
      gf.createPolygon(boundaryRing, holesRings)
    }
    typeId match {
      case POINT =>
        gf.createPoint(boundaries.head.map(_.toCoordinate).head)
      case MULTIPOINT =>
        gf.createMultiPointFromCoords(boundaries.map(p => p.head.toCoordinate))
      case LINESTRING =>
        // throw new NotImplementedError("LINESTRING not yet implemented in InternalGeometry")
        gf.createLineString(boundaries.head.map(_.toCoordinate))
      case MULTILINESTRING =>
        val lineStrings = boundaries.map(
          b => gf.createLineString(b.map(_.toCoordinate))
        )
        gf.createMultiLineString(lineStrings)
        // throw new NotImplementedError("MULTILINESTRING not yet implemented in InternalGeometry")
      case POLYGON =>
        createPolygon(0)
      case MULTIPOLYGON =>
        val polygons = for (i <- boundaries.indices) yield createPolygon(i)
        gf.createMultiPolygon(polygons.toArray)

    }
  }

  /**
   * Serialize for spark internals.
   * @return An instance of [[InternalRow]].
   */
  def serialize: Any = InternalRow.fromSeq(Seq(
      typeId,
      ArrayData.toArrayData(
        boundaries.map(boundary => ArrayData.toArrayData(boundary.map(_.serialize)))
      ),
      ArrayData.toArrayData(
        holes.map(
          holeGroup => ArrayData.toArrayData(
            holeGroup.map(hole => ArrayData.toArrayData(hole.map(_.serialize)))
          )
        )
      )
    )
  )
}

/** Companion object. */
object InternalGeometry {

  /**
   * Converts a Point to an instance of [[InternalGeometry]].
   * @param g An instance of Point to be converted.
   * @return An instance of [[InternalGeometry]].
   */
  private def fromPoint(g: Point, typeId: Int): InternalGeometry = {
    val shell = Array(InternalCoord(g.getCoordinate()))
    new InternalGeometry(typeId, Array(shell), Array(Array(Array())))
  }

  /**
   * Converts a LineString to an instance of [[InternalGeometry]].
   * @param g An instance of LineString to be converted.
   * @return An instance of [[InternalGeometry]].
   */
  private def fromLineString(g: LineString, typeId: Int): InternalGeometry = {
    val shell = g.getCoordinates.map(c => InternalCoord(c))
    new InternalGeometry(typeId, Array(shell), Array(Array(Array())))
  } 

  /**
   * Converts a Polygon to an instance of [[InternalGeometry]].
   * @param g An instance of Polygon to be converted.
   * @param typeName Type name to used to construct the instance
   *                 of [[InternalGeometry]].
   * @return An instance of [[InternalGeometry]].
   */
  private def fromPolygon(g: Polygon, typeId: Int): InternalGeometry = {
    val boundary = g.getBoundary
    val shell = boundary.getGeometryN(0).getCoordinates.map(InternalCoord(_))
    val holes = for (i <- 1 until boundary.getNumGeometries)
      yield boundary.getGeometryN(i).getCoordinates.map(InternalCoord(_))
    new InternalGeometry(typeId, Array(shell), Array(holes.toArray))
  }

  /**
   * Used for constructing a geometry collection by merging Point/MultiPoint,
   * LineString/MultiLineString or Polygon/MultiPolygon instances.
   * @param left An InternalGeometry instance.
   * @param right An InternalGeometry instance.
   * @return An InternalGeometry geometry collection created by combining left and right instances.
   */
  private def merge(left: InternalGeometry, right: InternalGeometry): InternalGeometry = {
    InternalGeometry(
      left.typeId max right.typeId,
      left.boundaries ++ right.boundaries,
      left.holes ++ right.holes
    )
  }

  /**
   * A smart constructor that construct an instance of [[InternalGeometry]]
   * based on an instance of [[Geometry]] corresponding to a Polygon or MultiPolygon.
   * @param g An instance of [[Geometry]].
   * @return An instance of [[InternalGeometry]].
   */
  def apply(g: Geometry): InternalGeometry = {
    g.getGeometryType match {
      case "Point" =>
        fromPoint(g.asInstanceOf[Point], POINT)
      case "MultiPoint" =>
        val geoms = for (i <- 0 until g.getNumGeometries)
          yield fromPoint(g.getGeometryN(i).asInstanceOf[Point], MULTIPOINT)
        geoms.reduce(merge)
      case "LineString" =>
        fromLineString(g.asInstanceOf[LineString], LINESTRING)
      case "MultiLineString" =>
        val geoms = for (i <- 0 until g.getNumGeometries)
          yield fromLineString(g.getGeometryN(i).asInstanceOf[LineString], MULTILINESTRING)
        geoms.reduce(merge)
      case "Polygon" =>
        fromPolygon(g.asInstanceOf[Polygon], POLYGON)
      case "MultiPolygon" =>
        val geoms = for (i <- 0 until g.getNumGeometries)
          yield fromPolygon(g.getGeometryN(i).asInstanceOf[Polygon], MULTIPOLYGON)
        geoms.reduce(merge)
    }
  }

  /**
   * A smart constructor that construct an instance of [[InternalGeometry]]
   * based on an instance of internal Spark data.
   * @param input An instance of [[InternalRow]].
   * @return An instance of [[InternalGeometry]].
   */
  def apply(input: InternalRow): InternalGeometry = {
    val typeId = input.getInt(0)

    val boundaries = input.getArray(1)
      .toObjectArray(ArrayType(ArrayType(InternalCoordType)))
      .map(
        _.asInstanceOf[ArrayData].toObjectArray(InternalCoordType)
        .map(c => InternalCoord(c.asInstanceOf[ArrayData]))
      )

    val holeGroups = input.getArray(2)
      .toObjectArray(ArrayType(ArrayType(ArrayType(InternalCoordType))))
      .map(
        _.asInstanceOf[ArrayData].toObjectArray(ArrayType(ArrayType(InternalCoordType)))
          .map(
            _.asInstanceOf[ArrayData].toObjectArray(ArrayType(ArrayType(InternalCoordType)))
            .map(c => InternalCoord(c.asInstanceOf[ArrayData])))
      )

    new InternalGeometry(typeId, boundaries, holeGroups)
  }

}
