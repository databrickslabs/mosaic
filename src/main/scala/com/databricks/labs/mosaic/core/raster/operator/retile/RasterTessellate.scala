package com.databricks.labs.mosaic.core.raster.operator.retile

import com.databricks.labs.mosaic.core.Mosaic
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.core.raster.gdal.MosaicRasterGDAL
import com.databricks.labs.mosaic.core.raster.operator.proj.RasterProject
import com.databricks.labs.mosaic.core.raster.operator.retile.ReTile.tileDataType
import com.databricks.labs.mosaic.core.types.model.MosaicRasterTile
import org.apache.spark.sql.types.{BinaryType, DataType, StringType}

/** RasterTessellate is a helper object for tessellating rasters. */
object RasterTessellate {

    val tileDataType: DataType = StringType // tessellate always uses checkpoint

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
                    (false, MosaicRasterTile(cell.index, null, tileDataType)) // invalid cellid
                } else {
                    val cellRaster = tmpRaster.getRasterForCell(cellID, indexSystem, geometryAPI)
                    if (!cellRaster.isEmpty) {
                        // copy to checkpoint dir (destroy cellRaster)
                        val checkpointPath = cellRaster.writeToCheckpointDir(doDestroy = true)
                        val newParentPath = cellRaster.createInfo("path")
                        (
                            true, // valid result
                            MosaicRasterTile(
                                cell.index,
                                MosaicRasterGDAL(
                                    null,
                                    cellRaster.createInfo + ("path" -> checkpointPath, "parentPath" -> newParentPath),
                                    -1),
                                tileDataType
                            )
                        )
                    } else {
                        (
                            false,
                            MosaicRasterTile(cell.index, cellRaster, tileDataType) // empty result
                        )
                    }
                }
            })

        val (result, invalid) = chips.partition(_._1) // true goes to result
        invalid.flatMap(t => Option(t._2.getRaster)).foreach(_.destroy()) // destroy invalids

        raster.destroy()
        tmpRaster.destroy()

        result.map(_._2) // return valid tiles
    }

}
