package com.databricks.labs.mosaic.core.raster.operator.retile

import com.databricks.labs.mosaic.{NO_PATH_STRING, RASTER_PARENT_PATH_KEY, RASTER_PATH_KEY}
import com.databricks.labs.mosaic.core.Mosaic
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.core.raster.gdal.RasterGDAL
import com.databricks.labs.mosaic.core.raster.operator.proj.RasterProject
import com.databricks.labs.mosaic.core.types.model.RasterTile
import com.databricks.labs.mosaic.functions.ExprConfig
import org.apache.spark.sql.types.{DataType, StringType}

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
      * @param exprConfigOpt
      *   Option [[ExprConfig]]
      * @return
      *   A sequence of MosaicRasterTile objects.
      */
    def tessellate(
                      raster: RasterGDAL,
                      resolution: Int,
                      indexSystem: IndexSystem,
                      geometryAPI: GeometryAPI,
                      exprConfigOpt: Option[ExprConfig]
                  ): Seq[RasterTile] = {

        val indexSR = indexSystem.osrSpatialRef
        val bbox = raster.bbox(geometryAPI, indexSR)
        val cells = Mosaic.mosaicFill(bbox, resolution, keepCoreGeom = false, indexSystem, geometryAPI)
        val tmpRaster = RasterProject.project(raster, indexSR, exprConfigOpt)

        val chips = cells
            .map(cell => {
                val cellID = cell.cellIdAsLong(indexSystem)
                val isValidCell = indexSystem.isValid(cellID)
                if (!isValidCell) {
                    (
                        false,
                        RasterTile(cell.index, RasterGDAL(), tileDataType)
                    ) // invalid cellid
                } else {
                    val cellRaster = tmpRaster.getRasterForCell(cellID, indexSystem, geometryAPI)
                    if (!cellRaster.isEmpty) {
                        (
                            true, // valid result
                            RasterTile(
                                cell.index,
                                cellRaster,
                                tileDataType
                            )
                        )
                    } else {
                        (
                            false,
                            RasterTile(cell.index, cellRaster, tileDataType) // empty result
                        )
                    }
                }
            })

        val (result, invalid) = chips.partition(_._1) // true goes to result
        invalid.flatMap(t => Option(t._2.raster)).foreach(_.flushAndDestroy()) // destroy invalids

        raster.flushAndDestroy()
        tmpRaster.flushAndDestroy()

        result.map(_._2) // return valid tiles
    }

}
