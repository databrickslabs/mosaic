package com.databricks.mosaic.core

import com.databricks.mosaic.types.model.MosaicChip
import org.locationtech.jts.geom.Geometry

/**
 * Single abstracted logic for mosaic fill via [[IndexSystem]].
 * [[IndexSystem]] is in charge of implementing the individual
 * steps of the logic.
 */
object Mosaic {

  def mosaicFill(geometry: Geometry, resolution: Int, indexSystem: IndexSystem): Seq[MosaicChip] = {

    val radius = indexSystem.getBufferRadius(geometry, resolution)

    val carvedGeometry = geometry.buffer(-radius)
    val borderGeometry = geometry.getBoundary.buffer(radius)

    val coreIndices = indexSystem.polyfill(carvedGeometry, resolution)
    val borderIndices = indexSystem.polyfill(borderGeometry, resolution)

    val coreChips = indexSystem.getCoreChips(coreIndices)
    val borderChips = indexSystem.getBorderChips(geometry, borderIndices)

    coreChips ++ borderChips
  }

}
