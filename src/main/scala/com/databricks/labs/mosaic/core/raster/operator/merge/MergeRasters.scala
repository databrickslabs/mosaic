package com.databricks.labs.mosaic.core.raster.operator.merge

import com.databricks.labs.mosaic.core.raster.gdal.MosaicRasterGDAL
import com.databricks.labs.mosaic.core.raster.io.RasterCleaner.dispose
import com.databricks.labs.mosaic.core.raster.operator.gdal.{GDALBuildVRT, GDALTranslate}
import com.databricks.labs.mosaic.utils.PathUtils

/** MergeRasters is a helper object for merging rasters. */
object MergeRasters {

    /**
      * Merges the rasters into a single raster.
      *
      * @param rasters
      *   The rasters to merge.
      * @return
      *   A MosaicRaster object.
      */
    def merge(rasters: Seq[MosaicRasterGDAL]): MosaicRasterGDAL = {
        val outOptions = rasters.head.getWriteOptions

        val vrtPath = PathUtils.createTmpFilePath("vrt")
        val rasterPath = PathUtils.createTmpFilePath(outOptions.extension)

        val vrtRaster = GDALBuildVRT.executeVRT(
          vrtPath,
          rasters,
          command = s"gdalbuildvrt -resolution highest"
        )

        val result = GDALTranslate.executeTranslate(
          rasterPath,
          vrtRaster,
          command = s"gdal_translate",
          outOptions
        )

        dispose(vrtRaster)

        result
    }

}
