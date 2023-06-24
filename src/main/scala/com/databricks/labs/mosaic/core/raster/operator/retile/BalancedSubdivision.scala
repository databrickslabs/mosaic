package com.databricks.labs.mosaic.core.raster.operator.retile

import com.databricks.labs.mosaic.core.raster.MosaicRaster

import scala.collection.immutable


object BalancedSubdivision {

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

    def splitRaster(
        mosaicRaster: MosaicRaster,
        numSplits: Int
    ): immutable.Seq[MosaicRaster] = {
        val (x, y) = mosaicRaster.getDimensions
        val (tileX, tileY) = getTileSize(x, y, numSplits)
        ReTile.reTile(mosaicRaster, tileX, tileY)
    }

}
