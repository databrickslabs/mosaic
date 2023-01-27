package com.databricks.labs.mosaic.core.types.model

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.index.IndexSystem
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{LongType, StringType}
import org.apache.spark.unsafe.types.UTF8String

/**
  * A case class modeling an instance of a mosaic chip. A chip can belong to
  * either core or border set.
  *
  * @param isCore
  *   Whether the chip belongs to the core set.
  * @param index
  *   Index ID.
  * @param geom
  *   Geometry instance if the chip is a border chip.
  */
case class MosaicChip(isCore: Boolean, index: Either[Long, String], geom: MosaicGeometry) {

    /**
      * Indicates whether the chip is outside of the representation of the
      * geometry it was generated to represent (ie false positive index).
      */
    def isEmpty: Boolean = !isCore & Option(geom).forall(_.isEmpty)

    /**
     * Formats the index ID as the data type supplied by the index system.
     *
     * @param indexSystem Index system to use for formatting.
     *
     * @return MosaicChip with formatted index ID.
     */
    def formatCellId(indexSystem: IndexSystem): MosaicChip = {
        (indexSystem.getCellIdDataType, index) match {
            case (_: LongType, Left(value))    => this
            case (_: StringType, Right(value)) => this
            case (_: LongType, Right(value))   => this.copy(index = Left(indexSystem.parse(value)))
            case (_: StringType, Left(value))  => this.copy(index = Right(indexSystem.format(value)))
            case _                             => throw new IllegalArgumentException("Invalid cell id data type")
        }
    }

    def cellIdAsLong(indexSystem: IndexSystem): Long = index match {
        case Left(value) => value
        case _           => indexSystem.parse(index.right.get)
    }

    def cellIdAsStr(indexSystem: IndexSystem): String = index match {
        case Right(value) => value
        case _            => indexSystem.format(index.left.get)
    }

    /**
      * Serialise to spark internal representation.
      *
      * @return
      *   An instance of [[InternalRow]].
      */
    def serialize: InternalRow = {
        if (index.isLeft) InternalRow.fromSeq(Seq(isCore, index.left.get, encodeGeom))
        else InternalRow.fromSeq(Seq(isCore, UTF8String.fromString(index.right.get), encodeGeom))
    }

    /**
      * Encodes the chip geometry as WKB.
      *
      * @return
      *   An instance of [[Array]] of [[Byte]] representing WKB.
      */
    private def encodeGeom: Array[Byte] = Option(geom).map(_.toWKB).orNull

}
