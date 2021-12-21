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

    // do not modify the radius
    val carvedGeometry = geometry.buffer(-radius)
    // add 1% to the radius to ensure union of carved and border geometries does not have holes inside the original geometry areas
    val borderGeometry = if(carvedGeometry.isEmpty) {
      geometry.buffer(radius*1.01)
    } else {
      geometry.getBoundary.buffer(radius*1.01)
    }

    val coreIndices = indexSystem.polyfill(carvedGeometry, resolution)
    val borderIndices = indexSystem.polyfill(borderGeometry, resolution)

    val coreChips = indexSystem.getCoreChips(coreIndices)
    val borderChips = indexSystem.getBorderChips(geometry, borderIndices)

    coreChips ++ borderChips
  }

}
