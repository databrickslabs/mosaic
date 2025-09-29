package com.databricks.labs.gbx.rasterx.operations

import org.gdal.gdal.Dataset

/** OverlappingTiles is a helper object for retiling rasters. */
object OverlappingTiles {

    def generateWindows(
        ds: Dataset,
        tileWidth: Int,
        tileHeight: Int,
        overlapPercentage: Int
    ): Seq[(Int, Int, Int, Int)] = {
        val (xSize, ySize) = (ds.getRasterXSize, ds.getRasterYSize)
        val overlapWidth = Math.ceil(tileWidth * overlapPercentage / 100.0).toInt
        val overlapHeight = Math.ceil(tileHeight * overlapPercentage / 100.0).toInt
        for {
            x <- 0 until xSize by (tileWidth - overlapWidth)
            y <- 0 until ySize by (tileHeight - overlapHeight)
        } yield {
            val xOffset = Math.min(tileWidth, xSize - x)
            val yOffset = Math.min(tileHeight, ySize - y)
            (x, y, xOffset, yOffset)
        }
    }

    /**
      * Retiles a raster into overlapping tiles.
      * @note
      *   The overlap percentage is a percentage of the tile size.
      *
      * @param ds
      *   The raster to retile.
      * @param tileWidth
      *   The width of the tiles.
      * @param tileHeight
      *   The height of the tiles.
      * @param overlapPercentage
      *   The percentage of overlap between tiles.
      * @return
      *   A sequence of Raster objects.
      */
    def reTileIter(
        ds: Dataset,
        options: Map[String, String],
        tileWidth: Int,
        tileHeight: Int,
        overlapPercentage: Int
    ): Iterator[(Dataset, Map[String, String])] = {
        val windows = generateWindows(ds, tileWidth, tileHeight, overlapPercentage)
        ReTile.reTileIter(ds, options, windows)
    }

}
