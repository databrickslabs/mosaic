package com.databricks.mosaic.core.types.model

import com.databricks.mosaic.core.geometry.MosaicGeometry
import org.apache.spark.sql.catalyst.InternalRow

/**
 * A case class modeling an instance of a mosaic chip.
 * A chip can belong to either core or border set.
 * @param isCore Whether the chip belongs to the core set.
 * @param index Index ID.
 * @param geom Geometry instance if the chip is a border chip.
 */
case class MosaicChip(isCore: Boolean, index: Long, geom: MosaicGeometry) {

  /**
   * Perform an intersection with a geometry, and if
   * intersection is non empty and the chip is not
   * a core set chip then extract the chip geometry.
   * @param other Geometry instance.
   * @return A Mosaic Chip instance.
   */
  def intersection(other: MosaicGeometry): MosaicChip = {
    val intersect = other.intersection(geom)
    val isCore = intersect.equals(geom)
    if (isCore) {
      MosaicChip(isCore, index, null)
    } else {
      MosaicChip(isCore, index, intersect)
    }
  }

  /** Indicates whether the chip is outside of the representation
   * of the geometry it was generated to represent (ie false positive index). */
  def isEmpty: Boolean = !isCore & Option(geom).forall(_.isEmpty)

  /**
   * Encodes the chip geometry as WKB.
   * @return An instance of [[Array]] of [[Byte]] representing WKB.
   */
  private def encodeGeom: Array[Byte] = Option(geom).map(_.toWKB).orNull

  /**
   * Serialise to spark internal representation.
   * @return An instance of [[InternalRow]].
   */
  def serialize: InternalRow = InternalRow.fromSeq(Seq(isCore, index, encodeGeom))
}
