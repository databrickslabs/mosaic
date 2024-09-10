package com.databricks.labs.mosaic.core.raster.operator.separate

import com.databricks.labs.mosaic.core.raster.io.RasterIO.createTmpFileFromDriver
import com.databricks.labs.mosaic.core.raster.operator.gdal.GDALTranslate
import com.databricks.labs.mosaic.core.types.model.RasterTile
import com.databricks.labs.mosaic.functions.ExprConfig
import org.apache.spark.sql.types.{DataType, StringType}

/**
  * ReTile is a helper object for splitting multi-band rasters into
  * single-band-per-row.
  * -
  */
object SeparateBands {

    val tileDataType: DataType = StringType // always use checkpoint

    /**
      * Separates tile bands into separate rasters. Empty bands are discarded.
      *
      * @param tile
      *   The tile to retile.
      * @param exprConfigOpt
      *   Option [[ExprConfig]]
      * @return
      *   A sequence of MosaicRasterTile objects.
      */
    def separate(
                    tile: => RasterTile,
                    exprConfigOpt: Option[ExprConfig]
    ): Seq[RasterTile] = {
        val raster = tile.raster

        val numBands = raster.numBands

        val tiles =
            if (numBands > 0) {
                // separate bands
                for (i <- 0 until raster.numBands) yield {
                    val driverShortName = raster.getDriverName()
                    val rasterPath = createTmpFileFromDriver(driverShortName, exprConfigOpt)
                    val outOptions = raster.getWriteOptions

                    val result = GDALTranslate.executeTranslate(
                        rasterPath,
                        raster,
                        command = s"gdal_translate -of $driverShortName -b ${i + 1}",
                        writeOptions = outOptions,
                        exprConfigOpt
                    ).tryInitAndHydrate() // <- required

                    if (!result.isEmpty) {
                        // update the band index
                        // both the variable and the metadata
                        val bandVal = (i + 1)
                        result.updateBandIdx(bandVal)
                        (true, result)

                    } else {
                        result.flushAndDestroy() // destroy inline for performance
                        (false, result) // empty result
                    }
                }
            } else {
                // no bands - just return the raster
                Seq((raster.isEmpty, raster))
            }

        val (result, _) = tiles.partition(_._1)
        result.map(t => new RasterTile(null, t._2, tileDataType))
    }

}
