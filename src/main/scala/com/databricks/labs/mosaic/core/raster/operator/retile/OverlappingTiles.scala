package com.databricks.labs.mosaic.core.raster.operator.retile

import com.databricks.labs.mosaic.core.raster.operator.gdal.GDALTranslate
import com.databricks.labs.mosaic.core.types.model.RasterTile
import com.databricks.labs.mosaic.functions.ExprConfig
import org.apache.spark.sql.types.{DataType, StringType}

import scala.collection.immutable

/** OverlappingTiles is a helper object for retiling rasters. */
object OverlappingTiles {

    //serialize data type (always use checkpoint)
    val tileDataType: DataType = StringType

    /**
      * Retiles a tile into overlapping tiles.
 *
      * @note
      *   The overlap percentage is a percentage of the tile size.
      * @param tile
      *   The tile to retile.
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

                val interim = GDALTranslate.executeTranslate(
                    rasterPath,
                    raster,
                    command = s"gdal_translate -srcwin $xOff $yOff $width $height",
                    outOptions,
                    exprConfigOpt
                ).tryInitAndHydrate() // <- required

                if (interim.isEmptyRasterGDAL || interim.isEmpty) {
                    interim.flushAndDestroy() // destroy inline for performance
                    (false, interim)
                } else {
                    (true, interim) // <- valid result
                }
            }
        }

        val (result, invalid) = tiles.flatten.partition(_._1) // true goes to result

        result.map(t => RasterTile(null, t._2, tileDataType)) // return valid tiles
    }

}
