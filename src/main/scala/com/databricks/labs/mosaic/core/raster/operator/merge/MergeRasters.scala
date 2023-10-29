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
    def merge(rasters: => Seq[MosaicRasterGDAL]): MosaicRasterGDAL = {
        val rasterUUID = java.util.UUID.randomUUID.toString
        val outShortName = rasters.head.getRaster.GetDriver.getShortName

        val vrtPath = PathUtils.createTmpFilePath(rasterUUID, "vrt")
        val rasterPath = PathUtils.createTmpFilePath(rasterUUID, "tif")

        val vrtRaster = GDALBuildVRT.executeVRT(
            vrtPath,
            isTemp = true,
            rasters,
            command = s"gdalbuildvrt -resolution highest"
        )

        val result = GDALTranslate.executeTranslate(
            rasterPath,
            isTemp = true,
            vrtRaster,
            command = s"gdal_translate -r bilinear -of $outShortName -co COMPRESS=DEFLATE"
        )

        dispose(vrtRaster)

        result
    }


}
