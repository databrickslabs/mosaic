package com.databricks.mosaic.types.model

import com.esri.core.geometry.ogc.OGCGeometry
import org.apache.spark.sql.catalyst.InternalRow

/**
 * A case class modeling an instance of a mosaic chip.
 * A chip can belong to either core or border set.
 * @param isCore Whether the chip belongs to the core set.
 * @param index Index ID.
 * @param geom Geometry instance if the chip is a border chip.
 */
case class MosaicChipESRI(isCore: Boolean, index: Long, geom: OGCGeometry) {

  /**
   * Perform an intersection with a geometry, and if
   * intersection is non empty and the chip is not
   * a core set chip then extract the chip geometry.
   * @param other Geometry instance.
   * @return A Mosaic Chip instance.
   */
  def intersection(other: OGCGeometry): MosaicChipESRI = {
    val intersect = other.intersection(geom)
    val isCore = intersect.Equals(geom)
    if (isCore) {
      MosaicChipESRI(isCore, index, null)
    } else {
      MosaicChipESRI(isCore, index, intersect)
    }
  }

  /** Indicates whether the chip is outside of the representation
   * of the geometry it was generated to represent (ie false positive index). */
  def isEmpty: Boolean = !isCore & Option(geom).forall(_.isEmpty)

  /**
   * Encodes the chip geometry as WKB.
   * @return An instance of [[Array]] of [[Byte]] representing WKB.
   */
  private def encodeGeom: Array[Byte] = Option(geom).map(_.asBinary().array()).orNull


  /**
   * Serialise to spark internal representation.
   * @return An instance of [[InternalRow]].
   */
  def serialize: InternalRow = InternalRow.fromSeq(Seq(isCore, index, encodeGeom))
}
