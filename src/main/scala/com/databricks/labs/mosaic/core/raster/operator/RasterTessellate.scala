package com.databricks.labs.mosaic.core.raster.operator

import com.databricks.labs.mosaic.core.Mosaic
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.core.raster.MosaicRaster
import com.databricks.labs.mosaic.core.types.model.MosaicRasterChip
import org.gdal.osr.SpatialReference

object RasterTessellate {

    def tessellate(raster: MosaicRaster, resolution: Int, indexSystem: IndexSystem, geometryAPI: GeometryAPI): Seq[MosaicRasterChip] = {
        val indexSR = indexSystem.osrSpatialRef
        val bbox = raster.bbox(geometryAPI, indexSR)
        val cells = Mosaic.mosaicFill(bbox, resolution, keepCoreGeom = false, indexSystem, geometryAPI)
        cells.map(cell => {
            val cellID = cell.cellIdAsLong(indexSystem)
            val cellRaster = raster.getRasterForCell(cellID, indexSystem, geometryAPI)
            MosaicRasterChip(cell.isCore, cell.index, cellRaster)
        })

    }

}
