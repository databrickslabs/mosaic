package com.databricks.labs.mosaic.core.raster.operator.retile

import com.databricks.labs.mosaic.core.Mosaic
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.core.raster.gdal.MosaicRasterGDAL
import com.databricks.labs.mosaic.core.raster.operator.proj.RasterProject
import com.databricks.labs.mosaic.core.types.model.MosaicRasterTile
import com.databricks.labs.mosaic.expressions.raster.base.RasterPathAware
import org.apache.spark.sql.types.{BinaryType, DataType}

/** RasterTessellate is a helper object for tessellating rasters. */
object RasterTessellate extends RasterPathAware{

    val tileDataType: DataType = BinaryType

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
      * @param manualMode
      *   Skip deletion of interim file writes, if any.
      * @return
      *   A sequence of MosaicRasterTile objects.
      */
    def tessellate(raster: MosaicRasterGDAL, resolution: Int, indexSystem: IndexSystem, geometryAPI: GeometryAPI,
                   manualMode: Boolean): Seq[MosaicRasterTile] = {
        val indexSR = indexSystem.osrSpatialRef
        val bbox = raster.bbox(geometryAPI, indexSR)
        val cells = Mosaic.mosaicFill(bbox, resolution, keepCoreGeom = false, indexSystem, geometryAPI)
        val tmpRaster = RasterProject.project(raster, indexSR)

        val chips = cells
            .map(cell => {
                val cellID = cell.cellIdAsLong(indexSystem)
                val isValidCell = indexSystem.isValid(cellID)
                if (!isValidCell) {
                    (false, MosaicRasterTile(cell.index, null, tileDataType))
                } else {
                    val cellRaster = tmpRaster.getRasterForCell(cellID, indexSystem, geometryAPI)
                    val isValidRaster = !cellRaster.isEmpty
                    (
                      isValidRaster,
                      MosaicRasterTile(cell.index, cellRaster, tileDataType)
                    )
                }
            })

        val (result, invalid) = chips.partition(_._1)
        invalid.flatMap(t => Option(t._2.getRaster)).foreach(
            pathSafeDispose(_, manualMode))

        pathSafeDispose(tmpRaster, manualMode)

        result.map(_._2)
    }

}
