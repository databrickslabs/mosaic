package com.databricks.labs.mosaic.expressions.raster.base

import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.core.raster.gdal.{RasterBandGDAL, RasterGDAL}

/**
  * Base trait for tile grid expressions. It provides the boilerplate code
  * needed to create a function builder for a given expression. It minimises
  * amount of code needed to create a new expression.
  */
trait RasterGridExpression {

    /**
      * Transforms a pixel to a cell ID and a value.
      * @param gt
      *   The geotransform of the tile.
      * @param indexSystem
      *   The index system to be used.
      * @param resolution
      *   The resolution of the index system.
      * @param x
      *   X coordinate of the pixel.
      * @param y
      *   Y coordinate of the pixel.
      * @param value
      *   The value of the pixel.
      * @return
      *   A tuple containing the cell ID and the value.
      */
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

    /**
      * Transforms a tile to a sequence of maps. Each map contains cell IDs
      * and values for a given band.
      * @param raster
      *   The tile to be transformed.
      * @param indexSystem
      *   The index system to be used.
      * @param resolution
      *   The resolution of the index system.
      * @return
      *   A sequence of maps. Each map contains cell IDs and values for a given, default is empty.
      *   band.
      */
    def griddedPixels(
                         raster: RasterGDAL,
                         indexSystem: IndexSystem,
                         resolution: Int
                     ): Seq[Map[Long, Seq[Double]]] = {
        raster.getGeoTransformOpt match {
            case Some(gt) =>
                val bandTransform = (band: RasterBandGDAL) => {
                    val results = band.transformValues[(Long, Double)] (pixelTransformer (gt, indexSystem, resolution), (0L, - 1.0) )
                    results
                        // Filter out default cells. We don't want to return them since they are masked in original tile.
                        // We use 0L as a dummy cell ID for default cells.
                        .map (row => row.filter (_._1 != 0L) )
                        .filterNot (_.isEmpty)
                        .flatten
                        .groupBy (_._1) // Group by cell ID.
                }
                val transformed = raster.transformBands (bandTransform)
                transformed.map (band => band.mapValues (values => values.map (_._2) ) )
            case _ => Seq.empty[Map[Long, Seq[Double]]]
        }
    }

}
