package com.databricks.labs.mosaic.core.raster.operator.gdal

import com.databricks.labs.mosaic.core.raster.gdal.MosaicRasterGDAL
import org.gdal.gdal.{BuildVRTOptions, gdal}

object GDALBuildVRT {

    def executeVRT(outputPath: String, isTemp: Boolean, rasters: Seq[MosaicRasterGDAL], command: String): MosaicRasterGDAL = {
        val args = command.split(" ")
        if (args.head == "gdalbuildvrt") {
            val vrtOptionsVec = OperatorOptions.parseOptions(command)
            val vrtOptions = new BuildVRTOptions(vrtOptionsVec)
            val result = gdal.BuildVRT(outputPath, rasters.map(_.getRaster).toArray, vrtOptions)
            // TODO: Figure out multiple parents, should this be an array?
            // VRT files are just meta files, mem size doesnt make much sense so we keep -1
            val mosaicRaster = MosaicRasterGDAL(result, outputPath, isTemp, rasters.head.getParentPath, "VRT", -1)
            mosaicRaster.flushCache()
        } else {
            throw new Exception("Not a valid GDAL Warp command.")
        }
    }

}
