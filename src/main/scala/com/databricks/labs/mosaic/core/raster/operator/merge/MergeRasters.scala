package com.databricks.labs.mosaic.core.raster.operator.merge

import com.databricks.labs.mosaic.core.raster.MosaicRaster
import com.databricks.labs.mosaic.core.raster.gdal_raster.RasterCleaner
import com.databricks.labs.mosaic.core.raster.operator.gdal.{GDALBuildVRT, GDALTranslate}
import com.databricks.labs.mosaic.utils.PathUtils

object MergeRasters {

    def merge(rasters: Seq[MosaicRaster]): MosaicRaster = {
        val vrtUUID = java.util.UUID.randomUUID.toString
        val rasterUUID = java.util.UUID.randomUUID.toString
        val outShortName = rasters.head.getRaster.GetDriver.getShortName

        //val vrtPath = s"/vsimem/$vrtUUID.vrt"
        //val rasterPath = s"/vsimem/$rasterUUID.tif"
        val vrtPath = PathUtils.createTmpFilePath(vrtUUID, "vrt")
        val rasterPath = PathUtils.createTmpFilePath(rasterUUID, "tif")

        val vrtRaster = GDALBuildVRT.executeVRT(vrtPath, rasters, command = s"gdalbuildvrt -r average -resolution highest -overwrite")

        // We use PACKBITS compression based on benchmarking done here: https://digital-geography.com/geotiff-compression-comparison/
        // PACKBITS is losseless compression and is the fastest compression algorithm for GeoTIFFs, but it does not compress as well as DEFLATE.
        // https://en.wikipedia.org/wiki/PackBits
        val result =
            GDALTranslate.executeTranslate(rasterPath, vrtRaster, command = s"gdal_translate -of $outShortName -co COMPRESS=PACKBITS")

        RasterCleaner.dispose(vrtRaster)

        result
    }

}
