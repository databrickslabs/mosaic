package com.databricks.labs.mosaic.core.raster.operator.retile

import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.core.raster.operator.gdal.GDALTranslate
import com.databricks.labs.mosaic.core.types.model.MosaicRasterTile
import com.databricks.labs.mosaic.utils.PathUtils

import scala.collection.immutable

object OverlappingTiles {

    def reTile(
        tile: MosaicRasterTile,
        tileWidth: Int,
        tileHeight: Int,
        overlapPercentage: Int
    ): immutable.Seq[MosaicRasterTile] = {
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

                val uuid = java.util.UUID.randomUUID.toString
                val fileExtension = GDAL.getExtension(tile.driver)
                val rasterPath = PathUtils.createTmpFilePath(uuid, fileExtension)
                val shortName = raster.getRaster.GetDriver.getShortName

                val result = GDALTranslate.executeTranslate(
                  rasterPath,
                  isTemp = true,
                  raster,
                  command = s"gdal_translate -of $shortName -srcwin $xOff $yOff $width $height"
                )

                result.flushCache()
                MosaicRasterTile(tile.index, result, tile.parentPath, tile.driver)
            }
        }

        tiles.flatten

    }

}
