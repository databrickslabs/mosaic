package com.databricks.labs.mosaic.core.raster.operator.retile

import com.databricks.labs.mosaic.core.raster.operator.gdal.GDALTranslate
import com.databricks.labs.mosaic.core.types.model.MosaicRasterTile
import com.databricks.labs.mosaic.utils.PathUtils

import scala.collection.immutable

object ReTile {

    def reTile(
        tile: MosaicRasterTile,
        tileWidth: Int,
        tileHeight: Int
    ): immutable.Seq[MosaicRasterTile] = {
        val raster = tile.raster
        val (xR, yR) = raster.getDimensions
        val xTiles = Math.ceil(xR / tileWidth).toInt
        val yTiles = Math.ceil(yR / tileHeight).toInt

        val tiles = for (x <- 0 until xTiles; y <- 0 until yTiles) yield {
            val xMin = if (x == 0) x * tileWidth else x * tileWidth - 1
            val yMin = if (y == 0) y * tileHeight else y * tileHeight - 1

            val rasterUUID = java.util.UUID.randomUUID.toString
            val fileExtension = raster.getRasterFileExtension
            val rasterPath = PathUtils.createTmpFilePath(rasterUUID, fileExtension)
            val shortDriver = raster.getRaster.GetDriver().getShortName

            val result = GDALTranslate.executeTranslate(
              rasterPath,
              isTemp = true,
              raster,
              command = s"gdal_translate -of $shortDriver -srcwin $xMin $yMin ${tileWidth + 1} ${tileHeight + 1}"
            )

            result.flushCache()
            result

        }

        tiles.map(MosaicRasterTile(null, _, raster.getParentPath, raster.getDriversShortName))

    }

}
