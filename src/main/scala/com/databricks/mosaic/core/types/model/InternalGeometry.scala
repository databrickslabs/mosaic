package com.databricks.mosaic.core.types.model

import org.locationtech.jts.geom.{Geometry, GeometryFactory, Polygon}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import com.databricks.mosaic.core.types.InternalCoordType
import org.locationtech.jts.geom.Point

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
   typeName: String,
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
    typeName match {
      case "Point" =>
        gf.createPoint(boundaries.head.map(_.toCoordinate).head)
      case "Polygon" =>
        createPolygon(0)
      case "MultiPolygon" =>
        val polygons = for (i <- boundaries.indices) yield createPolygon(i)
        gf.createMultiPolygon(polygons.toArray)
    }
  }

  /**
   * Serialize for spark internals.
   * @return An instance of [[InternalRow]].
   */
  def serialize: Any = InternalRow.fromSeq(Seq(
      UTF8String.fromString(typeName),
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
   * Converts a Polygon to an instance of [[InternalGeometry]].
   * @param g An instance of Polygon to be converted.
   * @param typeName Type name to used to construct the instance
   *                 of [[InternalGeometry]].
   * @return An instance of [[InternalGeometry]].
   */
  private def fromPolygon(g: Polygon, typeName: String): InternalGeometry = {
    val boundary = g.getBoundary
    val shell = boundary.getGeometryN(0).getCoordinates.map(InternalCoord(_))
    val holes = for (i <- 1 until boundary.getNumGeometries)
      yield boundary.getGeometryN(i).getCoordinates.map(InternalCoord(_))
    new InternalGeometry(typeName, Array(shell), Array(holes.toArray))
  }

  /**
   * Converts a Point to an instance of [[InternalGeometry]].
   * @param g An instance of Point to be converted.
   * @return An instance of [[InternalGeometry]].
   */
  private def fromPoint(g: Point): InternalGeometry = {
    val shell = Array(InternalCoord(g.getCoordinate()))
    new InternalGeometry("Point", Array(shell), Array(Array(Array())))
  }

  /**
   * Used for constructing a MultiPolygon instance by merging Polygon/MultiPolygon instances.
   * @param left An instance of a Polygon/MultiPolygon.
   * @param right An instance of a Polygon/MultiPolygon.
   * @return An instance of a MultiPolygon created by combining left and right instances.
   */
  private def merge(left: InternalGeometry, right: InternalGeometry): InternalGeometry = {
    InternalGeometry(
      "MultiPolygon",
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
        fromPoint(g.asInstanceOf[Point])
      case "Polygon" =>
        fromPolygon(g.asInstanceOf[Polygon], "Polygon")
      case "MultiPolygon" =>
        val geoms = for (i <- 0 until g.getNumGeometries)
          yield fromPolygon(g.getGeometryN(i).asInstanceOf[Polygon], "MultiPolygon")
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
    val typeName = input.get(0, StringType).asInstanceOf[UTF8String].toString

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

    new InternalGeometry(typeName, boundaries, holeGroups)
  }

}
