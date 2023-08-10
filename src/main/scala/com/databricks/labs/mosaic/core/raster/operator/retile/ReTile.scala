package com.databricks.labs.mosaic.core.raster.operator.retile

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.raster.MosaicRaster
import com.databricks.labs.mosaic.core.raster.api.RasterAPI
import com.databricks.labs.mosaic.core.raster.operator.clip.RasterClipByVector

import scala.collection.immutable

object ReTile {

    def reTile(
        raster: MosaicRaster,
        tileWidth: Int,
        tileHeight: Int,
        geometryAPI: GeometryAPI,
        rasterAPI: RasterAPI
    ): immutable.Seq[MosaicRaster] = {
        val (xR, yR) = raster.getDimensions
        val xTiles = Math.ceil(xR / tileWidth).toInt
        val yTiles = Math.ceil(yR / tileHeight).toInt

        val tiles = for (x <- 0 until xTiles; y <- 0 until yTiles) yield {
            val xMin = x * tileWidth
            val yMin = y * tileHeight

            val bbox = geometryAPI.createBbox(xMin, yMin, xMin + tileWidth, yMin + tileHeight)
                .mapXY((x, y) => rasterAPI.toWorldCoord(raster.getGeoTransform, x.toInt, y.toInt))

            RasterClipByVector.clip(raster, bbox, raster.getRaster.GetSpatialRef(), geometryAPI)

        }

        tiles

    }

}
