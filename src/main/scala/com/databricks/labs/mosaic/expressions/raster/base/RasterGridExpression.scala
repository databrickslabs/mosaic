package com.databricks.labs.mosaic.expressions.raster.base

import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.core.raster.{MosaicRaster, MosaicRasterBand}

trait RasterGridExpression {

    def pixelTransformer(
        gt: Seq[Double],
        indexSystem: IndexSystem,
        resolution: Int
    )(x: Int, y: Int, value: Double): (Long, Double) = {
        val offset = 0.5 // This centers the point to the pixel centroid
        val xOffset = offset + x
        val yOffset = offset + y
        val xGeo = gt.head + xOffset * gt(1) + yOffset * gt(2)
        val yGeo = gt(3) + xOffset * gt(4) + yOffset * gt(5)
        val cellID = indexSystem.pointToIndex(xGeo, yGeo, resolution)
        (cellID, value)
    }

    def griddedPixels(
        raster: MosaicRaster,
        indexSystem: IndexSystem,
        resolution: Int
    ): Seq[Map[Long, Seq[Double]]] = {
        val gt = raster.getRaster.GetGeoTransform()
        val bandTransform = (band: MosaicRasterBand) => {
            val results = band.transformValues[(Long, Double)](pixelTransformer(gt, indexSystem, resolution), (0L, -1.0))
            results
                // Filter out default cells. We don't want to return them since they are masked in original raster.
                // We use 0L as a dummy cell ID for default cells.
                .map(row => row.filter(_._1 != 0L))
                .filterNot(_.isEmpty)
                .flatten
                .groupBy(_._1) // Group by cell ID.
        }
        val transformed = raster.transformBands(bandTransform)
        transformed.map(
          band => band.mapValues(
            values => values.map(_._2)
          )
        )
    }

}
