package com.databricks.labs.mosaic.core.raster.operator.retile

import com.databricks.labs.mosaic.core.raster.gdal.RasterGDAL
import com.databricks.labs.mosaic.core.types.model.RasterTile
import com.databricks.labs.mosaic.functions.ExprConfig

/* ReTile is a helper object for retiling rasters. */
object BalancedSubdivision {

    /**
      * Gets the number of splits for a raster. The number of splits is
      * determined by the size of the raster and the desired size of the split
      * rasters. The number of splits is always a power of 4. This is a
      * heuristic method only due to compressions and other factors.
      * - 0.4.3 uses 0 as fallback.
      *
      * @param raster
      *   The raster to split.
      * @param destSize
      *   The desired size of the split rasters in MB.
      * @return
      *   The number of splits.
      */
    def getNumSplits(raster: RasterGDAL, destSize: Int): Int = {
        val testSize: Long  = raster.getMemSize match {
            case m if m > 0 => m
            case _          => raster.calcMemSize()
        }
        val size: Long = {
            if (testSize > -1) testSize
            else 0L
        }
        var n = 1
        val destSizeBytes: Long = destSize * 1000L * 1000L
        if (size > 0 && size > destSizeBytes) {
            while (true) {
                n *= 4
                if (size / n <= destSizeBytes) return n
            }
        }
        n
    }

    /**
      * Gets the tile size for a raster. The tile size is determined by the
      * number of splits. The tile size is always a power of 4. This is a
      * heuristic method only due to compressions and other factors.
      * @note
      *   Power of 2 is used to split the raster in each step but the number of
      *   splits is always a power of 4.
      *
      * @param x
      *   The x dimension of the raster.
      * @param y
      *   The y dimension of the raster.
      * @param numSplits
      *   The number of splits.
      * @return
      *   The tile size.
      */
    def getTileSize(x: Int, y: Int, numSplits: Int): (Int, Int) = {
        def split(tile: (Int, Int)): (Int, Int) = {
            val (a, b) = tile
            if (a > b) (a / 2, b) else (a, b / 2)
        }
        var tile = (x, y)
        val originRatio = x.toDouble / y.toDouble
        var i = 0
        while (Math.pow(2, i) < numSplits) {
            i += 1
            tile = split(tile)
        }
        val ratio = tile._1.toDouble / tile._2.toDouble
        // if the ratio is not maintained, split one more time
        // 0.1 is an arbitrary threshold to account for rounding errors
        if (Math.abs(originRatio - ratio) > 0.1) tile = split(tile)
        tile
    }

    /**
      * Splits a raster into multiple rasters. The number of splits is
      * determined by the size of the raster and the desired size of the split
      * rasters. The number of splits is always a power of 4. This is a
      * heuristic method only due to compressions and other factors.
      *
      * @param tile
      *   The raster to split.
      * @param sizeInMb
      *   The desired size of the split rasters in MB.
      * @param exprConfigOpt
      *   Option [[ExprConfig]]
      * @return
      *   A sequence of MosaicRaster objects.
      */
    def splitRaster(
        tile: RasterTile,
        sizeInMb: Int,
        exprConfigOpt: Option[ExprConfig]
    ): Seq[RasterTile] = {
        val raster = tile.raster
        val numSplits = getNumSplits(raster, sizeInMb)
        val (x, y) = raster.getDimensions
        val (tileX, tileY) = getTileSize(x, y, numSplits)

        ReTile.reTile(tile, tileX, tileY, exprConfigOpt)
    }

}
