package com.databricks.labs.mosaic.core.types.model

import com.databricks.labs.mosaic.core.types.InternalCoordType
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum._

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._

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

    /**
      * Used for constructing a MultiPolygon instance by merging
      * Polygon/MultiPolygon instances.
      *
      * @param other
      *   An instance of a Polygon/MultiPolygon.
      * @return
      *   An instance of a MultiPolygon created by combining left and right
      *   instances.
      */
    def merge(other: InternalGeometry): InternalGeometry = {
        InternalGeometry(
          MULTIPOLYGON.id,
          this.srid,
          this.boundaries ++ other.boundaries,
          this.holes ++ other.holes
        )
    }

    /**
      * Serialize for spark internals.
      *
      * @return
      *   An instance of [[InternalRow]].
      */
    def serialize: Any =
        InternalRow.fromSeq(
          Seq(
            typeId,
            srid,
            ArrayData.toArrayData(
              boundaries.map(boundary => ArrayData.toArrayData(boundary.map(_.serialize)))
            ),
            ArrayData.toArrayData(
              holes.map(holeGroup =>
                  ArrayData.toArrayData(
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
