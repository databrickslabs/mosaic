package com.databricks.labs.mosaic.core.raster.operator.retile

import com.databricks.labs.mosaic.core.Mosaic
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.core.raster.gdal.MosaicRasterGDAL
import com.databricks.labs.mosaic.core.raster.operator.proj.RasterProject
import com.databricks.labs.mosaic.core.types.model.MosaicRasterTile

object RasterTessellate {

    def tessellate(raster: MosaicRasterGDAL, resolution: Int, indexSystem: IndexSystem, geometryAPI: GeometryAPI): Seq[MosaicRasterTile] = {
        val indexSR = indexSystem.osrSpatialRef
        val bbox = raster.bbox(geometryAPI, indexSR)
        val cells = Mosaic.mosaicFill(bbox, resolution, keepCoreGeom = false, indexSystem, geometryAPI)
        val tmpRaster = RasterProject.project(raster, indexSR)

        val chips = cells
            .map(cell => {
                val cellID = cell.cellIdAsLong(indexSystem)
                val isValidCell = indexSystem.isValid(cellID)
                if (!isValidCell) {
                    (false, MosaicRasterTile(cell.index, null, "", ""))
                } else {
                    val cellRaster = tmpRaster.getRasterForCell(cellID, indexSystem, geometryAPI)
                    val isValidRaster = cellRaster.getBandStats.values.map(_("mean")).sum > 0 && !cellRaster.isEmpty
                    (
                      isValidRaster,
                      MosaicRasterTile(cell.index, cellRaster, raster.getParentPath, raster.getDriversShortName)
                    )
                }
            })

        val (result, invalid) = chips.partition(_._1)
        invalid.flatMap(t => Option(t._2.raster)).foreach(_.destroy())
        tmpRaster.destroy()

        result.map(_._2)
    }

}
