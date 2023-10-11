package com.databricks.labs.mosaic.core.raster.operator.gdal

import com.databricks.labs.mosaic.core.raster.MosaicRaster
import com.databricks.labs.mosaic.core.raster.gdal_raster.MosaicRasterGDAL
import org.gdal.gdal.{WarpOptions, gdal}

import java.nio.file.{Files, Paths}

object GDALWarp {

    def executeWarp(outputPath: String, isTemp: Boolean, rasters: Seq[MosaicRaster], command: String): MosaicRaster = {
        val args = command.split(" ")
        if (args.head == "gdalwarp") {
            // Test: gdal.ParseCommandLine(command)
            val warpOptionsVec = OperatorOptions.parseOptions(command)
            val warpOptions = new WarpOptions(warpOptionsVec)
            val result = gdal.Warp(outputPath, rasters.map(_.getRaster).toArray, warpOptions)
            // TODO: Figure out multiple parents, should this be an array?
            // Format will always be the same as the first raster
            val size = Files.size(Paths.get(outputPath))
            val mosaicRaster = MosaicRasterGDAL(
              result,
              outputPath,
              isTemp,
              rasters.head.getParentPath,
              rasters.head.getDriversShortName,
              size
            )
            mosaicRaster.flushCache()
        } else {
            throw new Exception("Not a valid GDAL Warp command.")
        }
    }

}
