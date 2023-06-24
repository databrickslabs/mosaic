package com.databricks.labs.mosaic.core.raster.operator.retile

import com.databricks.labs.mosaic.core.Mosaic
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.core.raster.MosaicRaster
import com.databricks.labs.mosaic.core.types.model.MosaicRasterChip

object RasterTessellate {

    def tessellate(raster: MosaicRaster, resolution: Int, indexSystem: IndexSystem, geometryAPI: GeometryAPI): Seq[MosaicRasterChip] = {
        val indexSR = indexSystem.osrSpatialRef
        val bbox = raster.bbox(geometryAPI, indexSR)
        val cells = Mosaic.mosaicFill(bbox, resolution, keepCoreGeom = false, indexSystem, geometryAPI)
        cells
            .map(cell => {
                val cellID = cell.cellIdAsLong(indexSystem)
                val cellRaster = raster.getRasterForCell(cellID, indexSystem, geometryAPI)
                cellRaster.getRaster.FlushCache()
                if (cell.isCore) (true, MosaicRasterChip(cell.index, cellRaster))
                else {
                    (
                      // If the cell is not core, we check if it has any data, if it doesn't we don't return it
                      cellRaster.getBands.exists { band => band.values.count(_ != band.noDataValue) + band.maskValues.count(_ != 0) != 0 },
                      MosaicRasterChip(cell.index, cellRaster)
                    )
                }
            })
            .filter(_._1)
            .map(_._2)

    }

}
