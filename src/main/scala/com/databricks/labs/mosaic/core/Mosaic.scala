package com.databricks.labs.mosaic.core

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.core.types.model.MosaicChip

/**
  * Single abstracted logic for mosaic fill via [[IndexSystem]]. [[IndexSystem]]
  * is in charge of implementing the individual steps of the logic.
  */
object Mosaic {

    def mosaicFill(geometry: MosaicGeometry, resolution: Int, indexSystem: IndexSystem, geometryAPI: GeometryAPI): Seq[MosaicChip] = {

        val radius = indexSystem.getBufferRadius(geometry, resolution, geometryAPI)

        // do not modify the radius
        val carvedGeometry = geometry.buffer(-radius)
        // add 1% to the radius to ensure union of carved and border geometries does not have holes inside the original geometry areas
        val borderGeometry =
            if (carvedGeometry.isEmpty) {
                geometry.buffer(radius * 1.01).simplify(0.01 * radius)
            } else {
                geometry.boundary.buffer(radius * 1.01).simplify(0.01 * radius)
            }

        val coreIndices = indexSystem.polyfill(carvedGeometry, resolution)
        val borderIndices = indexSystem.polyfill(borderGeometry, resolution)

        val coreChips = indexSystem.getCoreChips(coreIndices)
        val borderChips = indexSystem.getBorderChips(geometry, borderIndices, geometryAPI)

        coreChips ++ borderChips
    }

}
