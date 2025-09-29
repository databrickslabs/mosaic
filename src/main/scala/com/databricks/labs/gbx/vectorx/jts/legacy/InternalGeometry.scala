package com.databricks.labs.gbx.vectorx.jts.legacy

import com.databricks.labs.gbx.vectorx.jts.{GeometryTypeEnum, JTS}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.locationtech.jts.geom.Geometry

/**
  * A case class modeling Polygons and MultiPolygons.
  *
  * @param typeId
  *   Type id indicting which enum value represents this geometry.
  * @param boundaries
  *   A collection of boundaries, for Polygon the length is 1, for MultiPolygon
  *   the collection will contain boundaries of each sub Polygon.
  * @param holes
  *   A collection of hole collections, for Polygon the length is 1, for
  *   MultiPolygon the collection will contain hole collection for each sub
  *   Polygon.
  */
case class InternalGeometry(
    typeId: Int,
    srid: Int,
    boundaries: Array[Array[InternalCoord]],
    holes: Array[Array[Array[InternalCoord]]]
) {

    def toJTS: Geometry = {
        GeometryTypeEnum.fromTypeId(typeId) match {
            case GeometryTypeEnum.POINT              => JTS.point(boundaries.head.head.toCoordinate)
            case GeometryTypeEnum.MULTIPOINT         => JTS.multiPoint(boundaries.head.map(p => JTS.point(p.toCoordinate)))
            case GeometryTypeEnum.LINESTRING         => JTS.lineStringXYs(boundaries.head.map(c => (c.coords(0), c.coords(1))).toBuffer)
            case GeometryTypeEnum.MULTILINESTRING    =>
                JTS.multiLineString(boundaries.map(ls => JTS.lineStringXYs(ls.map(c => (c.coords(0), c.coords(1))).toBuffer)))
            case GeometryTypeEnum.POLYGON            =>
                // TODO: handle holes
                JTS.polygonFromCoords(boundaries.head.map(c => c.toCoordinate))
            case GeometryTypeEnum.MULTIPOLYGON       =>
                // TODO: handle holes
                JTS.multiPolygonFromXYs(boundaries.map(_.map(c => (c.coords(0), c.coords(1))).toArray))
            case GeometryTypeEnum.GEOMETRYCOLLECTION =>
                // TODO: implement
                throw new IllegalAccessException("GeometryCollection not implemented")
        }
    }

}

/** Companion object. */
object InternalGeometry {

    val InternalCoordType: DataType = ArrayType.apply(DoubleType)

    /**
      * A smart constructor that construct an instance of [[InternalGeometry]]
      * based on an instance of internal Spark data.
      *
      * @param input
      *   An instance of [[InternalRow]].
      * @return
      *   An instance of [[InternalGeometry]].
      */
    def apply(input: InternalRow): InternalGeometry = {
        val typeId = input.getInt(0)
        val srid = input.getInt(1)

        val boundaries = input
            .getArray(2)
            .toObjectArray(ArrayType(ArrayType(InternalCoordType)))
            .map(
              _.asInstanceOf[ArrayData]
                  .toObjectArray(InternalCoordType)
                  .map(c => InternalCoord(c.asInstanceOf[ArrayData]))
            )

        val holeGroups = input
            .getArray(3)
            .toObjectArray(ArrayType(ArrayType(ArrayType(InternalCoordType))))
            .map(
              _.asInstanceOf[ArrayData]
                  .toObjectArray(ArrayType(ArrayType(InternalCoordType)))
                  .map(
                    _.asInstanceOf[ArrayData]
                        .toObjectArray(ArrayType(ArrayType(InternalCoordType)))
                        .map(c => InternalCoord(c.asInstanceOf[ArrayData]))
                  )
            )

        new InternalGeometry(typeId, srid, boundaries, holeGroups)
    }

}
