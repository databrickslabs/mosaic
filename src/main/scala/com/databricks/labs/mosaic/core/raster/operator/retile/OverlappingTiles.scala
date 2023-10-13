package com.databricks.labs.mosaic.core.raster.operator.retile

import com.databricks.labs.mosaic.core.raster.MosaicRaster
import com.databricks.labs.mosaic.core.raster.operator.gdal.GDALTranslate
import com.databricks.labs.mosaic.utils.PathUtils

import scala.collection.immutable

object OverlappingTiles {

    def reTile(
        raster: MosaicRaster,
        tileWidth: Int,
        tileHeight: Int,
        overlapPercentage: Int
    ): immutable.Seq[MosaicRaster] = {
        val (xSize, ySize) = raster.getDimensions

        val overlapWidth = Math.ceil(tileWidth * overlapPercentage / 100.0).toInt
        val overlapHeight = Math.ceil(tileHeight * overlapPercentage / 100.0).toInt

        val tiles = for (i <- 0 until xSize by (tileWidth - overlapWidth)) yield {
            for (j <- 0 until ySize by (tileHeight - overlapHeight)) yield {
                val xOff = if (i == 0) i else i - 1
                val yOff = if (j == 0) j else j - 1
                val width = Math.min(tileWidth, xSize - i) + 1
                val height = Math.min(tileHeight, ySize - j) + 1

                val fileExtension = raster.getExtension
                val rasterPath = PathUtils.createTmpFilePath(fileExtension)
                val shortName = raster.getRaster.GetDriver.getShortName

                val result = GDALTranslate.executeTranslate(
                    rasterPath,
                    isTemp = true,
                    raster,
                    command = s"gdal_translate -of $shortName -srcwin $xOff $yOff $width $height"
                )

                result.flushCache()
            }
        }

        tiles.flatten


    }

}
