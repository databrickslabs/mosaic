package com.databricks.labs.mosaic.core.raster.operator.merge

import com.databricks.labs.mosaic.core.raster.gdal.RasterGDAL
import com.databricks.labs.mosaic.core.raster.operator.gdal.{GDALBuildVRT, GDALTranslate}
import com.databricks.labs.mosaic.functions.ExprConfig
import com.databricks.labs.mosaic.utils.PathUtils

/** MergeRasters is a helper object for merging rasters. */
object MergeRasters {

    /**
      * Merges the rasters into a single tile.
      *
      * @param rasters
      *   The rasters to merge.
      * @param exprConfigOpt
     *    Option [[ExprConfig]]
      * @return
      *   A Raster object.
      */
    def merge(rasters: Seq[RasterGDAL], exprConfigOpt: Option[ExprConfig]): RasterGDAL = {
        val outOptions = rasters.head.getWriteOptions

        val vrtPath = PathUtils.createTmpFilePath("vrt", exprConfigOpt)
        val rasterPath = PathUtils.createTmpFilePath(outOptions.extension, exprConfigOpt)

        val vrtRaster = GDALBuildVRT.executeVRT(
            vrtPath,
            rasters,
            command = s"gdalbuildvrt -resolution highest",
            exprConfigOpt
        )

        val result = GDALTranslate.executeTranslate(
            rasterPath,
            vrtRaster,
            command = s"gdal_translate",
            outOptions,
            exprConfigOpt
        )

        vrtRaster.flushAndDestroy()

        result
    }

}
