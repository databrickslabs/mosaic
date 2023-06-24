package com.databricks.labs.mosaic.core.raster.operator.gdal

import com.databricks.labs.mosaic.core.raster.MosaicRaster
import com.databricks.labs.mosaic.core.raster.gdal_raster.{MosaicRasterGDAL, RasterCleaner}
import org.gdal.gdal.{WarpOptions, gdal}

object GDALWarp {

    def executeWarp(outputPath: String, rasters: Seq[MosaicRaster], command: String): MosaicRaster = {
        val args = command.split(" ")
        if (args.head == "gdalwarp") {
            // Test: gdal.ParseCommandLine(command)
            val warpOptionsVec = OperatorOptions.parseOptions(command)
            val warpOptions = new WarpOptions(warpOptionsVec)
            val result = gdal.Warp(outputPath, rasters.map(_.getRaster).toArray, warpOptions)
            MosaicRasterGDAL(result, outputPath)
        } else {
            throw new Exception("Not a valid GDAL Warp command.")
        }
    }

}
