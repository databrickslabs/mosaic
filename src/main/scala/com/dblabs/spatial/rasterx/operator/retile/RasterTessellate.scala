package com.dblabs.spatial.rasterx.operator.retile

import com.databricks.labs.mosaic.core.Mosaic
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.core.raster.gdal.MosaicRasterGDAL
import com.databricks.labs.mosaic.core.raster.io.RasterCleaner.dispose
import com.databricks.labs.mosaic.core.types.model.MosaicRasterTile

/** RasterTessellate is a helper object for tessellating rasters. */
object RasterTessellate {

    /**
      * Tessellates a raster into tiles. The raster is projected into the index
      * system and then split into tiles. Each tile corresponds to a cell in the
      * index system.
      *
      * @param raster
      *   The raster to tessellate.
      * @param resolution
      *   The resolution of the tiles.
      * @param indexSystem
      *   The index system to use.
      * @param geometryAPI
      *   The geometry API to use.
      * @return
      *   A sequence of MosaicRasterTile objects.
      */
    def tessellate(
        raster: MosaicRasterGDAL,
        resolution: Int,
        indexSystem: IndexSystem,
        geometryAPI: GeometryAPI
    ): Iterator[MosaicRasterTile] = {
        val indexSR = indexSystem.osrSpatialRef
        val bbox = raster.bbox(geometryAPI, indexSR)
        val cells = Mosaic.mosaicFill(bbox, resolution, keepCoreGeom = false, indexSystem, geometryAPI)

        // this cannot be an iterator as it depends on the tmp raster
        // and the tmp raster is disposed after the iterator is created
        // so we need to work in a sequence to ensure the raster is alive while we are processing the cells
        val chips = cells
            .flatMap(cell => {
                val cellID = cell.cellIdAsLong(indexSystem)
                if (!indexSystem.isValid(cellID)) None
                else {
                    try {
                        val cellRaster = raster.getRasterForCell(cellID, indexSystem, geometryAPI)
                        if (cellRaster.isEmpty) {
                            dispose(cellRaster)
                            None
                        } else {
                            Some(MosaicRasterTile(cell.index, cellRaster))
                        }
                    } catch {
                        case e: Throwable =>
                            val gdalError = org.gdal.gdal.gdal.GetLastErrorMsg()
                            if (gdalError.contains("sizes must be larger than zero")) {
                                // expected error when after reprojecting cell intersects by less than 1px
                                None
                            } else {
                                // rethrow the error
                                throw e
                            }
                    }
                }
            })
        chips.iterator
    }

}
