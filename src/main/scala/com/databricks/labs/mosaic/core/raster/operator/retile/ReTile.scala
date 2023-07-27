package com.databricks.labs.mosaic.core.raster.operator.retile

import com.databricks.labs.mosaic.core.raster.MosaicRaster
import com.databricks.labs.mosaic.core.raster.operator.gdal.GDALTranslate
import com.databricks.labs.mosaic.utils.PathUtils

import scala.collection.immutable

object ReTile {

    def reTile(
        raster: MosaicRaster,
        tileWidth: Int,
        tileHeight: Int
    ): immutable.Seq[MosaicRaster] = {
        val (xR, yR) = raster.getDimensions
        val xTiles = Math.ceil(xR / tileWidth).toInt
        val yTiles = Math.ceil(yR / tileHeight).toInt

        val tiles = for (x <- 0 until xTiles; y <- 0 until yTiles) yield {
            val xMin = x * tileWidth
            val yMin = y * tileHeight

            val uuid = java.util.UUID.randomUUID()
            //val resultFileName = s"/vsimem/${raster.uuid}_${uuid.toString}.${raster.getExtension}"
            val resultFileName = PathUtils.createTmpFilePath(raster.uuid.toString, raster.getExtension)

            GDALTranslate.executeTranslate(resultFileName, raster, s"gdal_translate -srcwin $xMin $yMin $tileWidth $tileHeight")
        }

        tiles

    }

}
