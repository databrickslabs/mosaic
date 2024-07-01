package com.databricks.labs.mosaic.core.raster.operator.retile

import com.databricks.labs.mosaic.core.raster.operator.gdal.GDALTranslate
import com.databricks.labs.mosaic.core.types.model.RasterTile
import com.databricks.labs.mosaic.functions.ExprConfig
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
        tile: RasterTile,
        tileWidth: Int,
        tileHeight: Int,
        exprConfigOpt: Option[ExprConfig]
    ): Seq[RasterTile] = {
        val raster = tile.raster
        val (xR, yR) = raster.getDimensions
        val xTiles = Math.ceil(xR / tileWidth).toInt
        val yTiles = Math.ceil(yR / tileHeight).toInt

        val tiles = for (x <- 0 until xTiles; y <- 0 until yTiles) yield {
            val xMin = x * tileWidth
            val yMin = y * tileHeight
            val xOffset = if (xMin + tileWidth > xR) xR - xMin else tileWidth
            val yOffset = if (yMin + tileHeight > yR) yR - yMin else tileHeight

            val rasterPath = raster.createTmpFileFromDriver(exprConfigOpt) // <- no mosaic config
            val outOptions = raster.getWriteOptions

            val result = GDALTranslate.executeTranslate(
                rasterPath,
                raster,
                command = s"gdal_translate -srcwin $xMin $yMin $xOffset $yOffset",
                outOptions,
                exprConfigOpt
            )

            if (!result.isEmpty) {
                (true, result)
            } else {
                result.flushAndDestroy() // destroy inline for performance
                (false, result) // empty result
            }
        }

        val (result, invalid) = tiles.partition(_._1) // true goes to result
//        invalid.flatMap(t => Option(t._2)).foreach(_.destroy()) // destroy invalids
        result.map(t => RasterTile(null, t._2, tileDataType)) // return valid tiles
    }

}
