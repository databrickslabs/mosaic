package com.databricks.labs.mosaic.core.raster.operator.retile

import com.databricks.labs.mosaic.core.Mosaic
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.core.raster.MosaicRaster
import com.databricks.labs.mosaic.core.raster.operator.proj.RasterProject
import com.databricks.labs.mosaic.core.types.model.MosaicRasterChip

object RasterTessellate {

    def tessellate(raster: MosaicRaster, resolution: Int, indexSystem: IndexSystem, geometryAPI: GeometryAPI): Seq[MosaicRasterChip] = {
        val indexSR = indexSystem.osrSpatialRef
        val bbox = raster.bbox(geometryAPI, indexSR)
        val cells = Mosaic.mosaicFill(bbox, resolution, keepCoreGeom = false, indexSystem, geometryAPI)
        val tmpRaster = RasterProject.project(raster, indexSR)
        val result = cells
            .map(cell => {
                val cellID = cell.cellIdAsLong(indexSystem)
                val cellRaster = tmpRaster.getRasterForCell(cellID, indexSystem, geometryAPI)
                cellRaster.getRaster.FlushCache()
                (
                  cellRaster.getBandStats.values.map(_("mean")).sum > 0 && !cellRaster.isEmpty,
                  MosaicRasterChip(cell.index, cellRaster)
                )
            })
            .filter(_._1)
            .map(_._2)
        tmpRaster.destroy()
        result
    }

}
