package com.databricks.labs.mosaic.core.raster.operator.retile

import com.databricks.labs.mosaic.core.raster.io.RasterCleaner.dispose
import com.databricks.labs.mosaic.core.raster.operator.gdal.{GDALBuildVRT, GDALTranslate}
import com.databricks.labs.mosaic.core.types.model.MosaicRasterTile
import com.databricks.labs.mosaic.utils.PathUtils

/** ReTile is a helper object for retiling rasters. */
object ReTile {

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
        val raster = tile.getRaster
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
            val shortDriver = raster.getDriversShortName

            val result = GDALTranslate.executeTranslate(
              rasterPath,
              raster,
              command = s"gdal_translate -of $shortDriver -srcwin $xMin $yMin $xOffset $yOffset -co COMPRESS=DEFLATE"
            )

            val isEmpty = result.isEmpty

            if (isEmpty) dispose(result)

            (isEmpty, result)

        }

        val (_, valid) = tiles.partition(_._1)

        valid.map(t => MosaicRasterTile(null, t._2, raster.getParentPath, raster.getDriversShortName))

    }

}
