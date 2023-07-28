package com.databricks.labs.mosaic.core.raster.operator.gdal

import com.databricks.labs.mosaic.core.raster.MosaicRaster
import com.databricks.labs.mosaic.core.raster.gdal_raster.{MosaicRasterGDAL, RasterCleaner}
import org.gdal.gdal.{BuildVRTOptions, Dataset, WarpOptions, gdal}

object GDALBuildVRT {

    def executeVRT(outputPath: String, isTemp: Boolean, rasters: Seq[MosaicRaster], command: String): MosaicRaster = {
        val args = command.split(" ")
        if (args.head == "gdalbuildvrt") {
            val vrtOptionsVec = OperatorOptions.parseOptions(command)
            val vrtOptions = new BuildVRTOptions(vrtOptionsVec)
            val result = gdal.BuildVRT(outputPath, rasters.map(_.getRaster).toArray, vrtOptions)
            val mosaicRaster = MosaicRasterGDAL(result, outputPath, isTemp)
            mosaicRaster.flushCache()
        } else {
            throw new Exception("Not a valid GDAL Warp command.")
        }
    }

}
