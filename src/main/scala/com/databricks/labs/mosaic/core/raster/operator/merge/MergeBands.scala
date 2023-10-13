package com.databricks.labs.mosaic.core.raster.operator.merge

import com.databricks.labs.mosaic.core.raster.MosaicRaster
import com.databricks.labs.mosaic.core.raster.operator.gdal.{GDALBuildVRT, GDALTranslate}
import com.databricks.labs.mosaic.utils.PathUtils

object MergeBands {

    def merge(rasters: Seq[MosaicRaster], resampling: String): MosaicRaster = {
        val outShortName = rasters.head.getRaster.GetDriver.getShortName

        val vrtPath = PathUtils.createTmpFilePath("vrt")
        val rasterPath = PathUtils.createTmpFilePath("tif")

        val vrtRaster = GDALBuildVRT.executeVRT(
          vrtPath,
          isTemp = true,
          rasters,
          command = s"gdalbuildvrt -separate -resolution highest"
        )

        val result = GDALTranslate.executeTranslate(
          rasterPath,
          isTemp = true,
          vrtRaster,
          command = s"gdal_translate -r $resampling -of $outShortName -co COMPRESS=PACKBITS"
        )

        result
    }

    def merge(rasters: Seq[MosaicRaster], pixel: (Double, Double), resampling: String): MosaicRaster = {
        val outShortName = rasters.head.getRaster.GetDriver.getShortName

        val vrtPath = PathUtils.createTmpFilePath("vrt")
        val rasterPath = PathUtils.createTmpFilePath("tif")

        val vrtRaster = GDALBuildVRT.executeVRT(
            vrtPath,
            isTemp = true,
            rasters,
            command = s"gdalbuildvrt -separate -resolution user -tr ${pixel._1} ${pixel._2}"
        )

        val result = GDALTranslate.executeTranslate(
            rasterPath,
            isTemp = true,
            vrtRaster,
            command = s"gdalwarp -r $resampling -of $outShortName -co COMPRESS=PACKBITS -overwrite"
        )

        result
    }

}
