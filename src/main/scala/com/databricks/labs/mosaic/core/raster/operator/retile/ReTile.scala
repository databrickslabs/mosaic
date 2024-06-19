package com.databricks.labs.mosaic.core.raster.operator.retile

import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.core.raster.gdal.MosaicRasterGDAL
import com.databricks.labs.mosaic.core.raster.operator.gdal.GDALTranslate
import com.databricks.labs.mosaic.core.types.model.MosaicRasterTile
import com.databricks.labs.mosaic.utils.PathUtils
import org.apache.spark.sql.types.{DataType, StringType}

/** ReTile is a helper object for retiling rasters. */
object ReTile {

    val tileDataType: DataType = StringType // always use checkpoint

    /**
      * Retiles a raster into tiles. Empty tiles are discarded. The tile size is
      * specified by the user via the tileWidth and tileHeight parameters.
      *
      * @param tile
      *   The raster to retile.
      * @param tileWidth
      *   The width of the tiles.
      * @param tileHeight
      *   The height of the tiles.
      * @return
      *   A sequence of MosaicRasterTile objects.
      */
    def reTile(
        tile: MosaicRasterTile,
        tileWidth: Int,
        tileHeight: Int
    ): Seq[MosaicRasterTile] = {
        val raster = tile.getRaster.withHydratedDataset()
        val (xR, yR) = raster.getDimensions
        val xTiles = Math.ceil(xR / tileWidth).toInt
        val yTiles = Math.ceil(yR / tileHeight).toInt

        val tiles = for (x <- 0 until xTiles; y <- 0 until yTiles) yield {
            val xMin = x * tileWidth
            val yMin = y * tileHeight
            val xOffset = if (xMin + tileWidth > xR) xR - xMin else tileWidth
            val yOffset = if (yMin + tileHeight > yR) yR - yMin else tileHeight

            val fileExtension = raster.getRasterFileExtension
            val rasterPath = PathUtils.createTmpFilePath(fileExtension)
            val outOptions = raster.getWriteOptions

            val result = GDALTranslate.executeTranslate(
              rasterPath,
              raster,
              command = s"gdal_translate -srcwin $xMin $yMin $xOffset $yOffset",
                outOptions
            )

            if (!result.isEmpty) {
                // copy to checkpoint dir
                val checkpointPath = result.writeToCheckpointDir(doDestroy = true)
                val newParentPath = result.createInfo("path")
                (true, MosaicRasterGDAL(null, result.createInfo + ("path" -> checkpointPath, "parentPath" -> newParentPath), -1))
            } else {
                result.destroy() // destroy inline for performance
                (false, result) // empty result
            }
        }
        raster.destroy()

        val (result, invalid) = tiles.partition(_._1) // true goes to result
//        invalid.flatMap(t => Option(t._2)).foreach(_.destroy()) // destroy invalids
        result.map(t => MosaicRasterTile(null, t._2, tileDataType)) // return valid tiles
    }

}
