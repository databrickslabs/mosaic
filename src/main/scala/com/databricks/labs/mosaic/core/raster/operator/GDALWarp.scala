package com.databricks.labs.mosaic.core.raster.operator

import com.databricks.labs.mosaic.core.raster.MosaicRaster
import org.gdal.gdal.{Dataset, WarpOptions, gdal}

object GDALWarp {

    def executeWarp(outputPath: String, raster: MosaicRaster, command: String): Dataset = {
        val args = command.split(" ")
        if (args.head == "gdalwarp") {
            val warpOptionsVec = new java.util.Vector[String]()
            args.drop(1).foreach(
              arg => warpOptionsVec.add(arg)
            )
            val warpOptions = new WarpOptions(warpOptionsVec)
            val result = gdal.Warp(outputPath, Array(raster.getRaster), warpOptions)
            result.FlushCache()
            result
        } else {
            throw new Exception("Not a valid GDAL Warp command.")
        }
    }

}
