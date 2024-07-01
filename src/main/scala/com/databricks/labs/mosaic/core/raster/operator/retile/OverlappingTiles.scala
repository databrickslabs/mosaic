package com.databricks.labs.mosaic.core.raster.operator.retile

import com.databricks.labs.mosaic.{NO_PATH_STRING, RASTER_PARENT_PATH_KEY, RASTER_PATH_KEY}
import com.databricks.labs.mosaic.core.raster.gdal.RasterGDAL
import com.databricks.labs.mosaic.core.raster.operator.gdal.GDALTranslate
import com.databricks.labs.mosaic.core.types.model.RasterTile
import com.databricks.labs.mosaic.functions.ExprConfig
import org.apache.spark.sql.types.{DataType, StringType}

import scala.collection.immutable

/** OverlappingTiles is a helper object for retiling rasters. */
object OverlappingTiles {

    //serialize data type
    val tileDataType: DataType = StringType // always use checkpoint

    /**
      * Retiles a raster into overlapping tiles.
 *
      * @note
      *   The overlap percentage is a percentage of the tile size.
      * @param tile
      *   The raster to retile.
      * @param tileWidth
      *   The width of the tiles.
      * @param tileHeight
      *   The height of the tiles.
      * @param overlapPercentage
      *   The percentage of overlap between tiles.
      * @return
      *   A sequence of RasterTile objects.
      */
    def reTile(
                  tile: RasterTile,
                  tileWidth: Int,
                  tileHeight: Int,
                  overlapPercentage: Int,
                  exprConfigOpt: Option[ExprConfig]
    ): immutable.Seq[RasterTile] = {
        val raster = tile.raster
        val (xSize, ySize) = raster.getDimensions

        val overlapWidth = Math.ceil(tileWidth * overlapPercentage / 100.0).toInt
        val overlapHeight = Math.ceil(tileHeight * overlapPercentage / 100.0).toInt

        val tiles = for (i <- 0 until xSize by (tileWidth - overlapWidth)) yield {
            for (j <- 0 until ySize by (tileHeight - overlapHeight)) yield {
                val xOff = i
                val yOff = j
                val width = Math.min(tileWidth, xSize - i)
                val height = Math.min(tileHeight, ySize - j)
                val rasterPath = raster.createTmpFileFromDriver(exprConfigOpt)
                val outOptions = raster.getWriteOptions

                val result = GDALTranslate.executeTranslate(
                    rasterPath,
                    raster,
                    command = s"gdal_translate -srcwin $xOff $yOff $width $height",
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
        }

        val (result, invalid) = tiles.flatten.partition(_._1) // true goes to result
        //        invalid.flatMap(t => Option(t._2)).foreach(_.destroy()) // destroy invalids
        result.map(t => RasterTile(null, t._2, tileDataType)) // return valid tiles

    }

}
