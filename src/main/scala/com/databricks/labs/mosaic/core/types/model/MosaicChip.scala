package com.databricks.labs.mosaic.core.types.model

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.index.IndexSystem
import org.apache.spark.sql.catalyst.InternalRow
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

    def toStringID(indexSystem: IndexSystem): MosaicChip = MosaicChip(isCore, Right(indexSystem.format(index.left.get)), geom)

}
