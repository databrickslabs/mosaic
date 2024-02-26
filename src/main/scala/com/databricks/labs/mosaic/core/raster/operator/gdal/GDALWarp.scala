package com.databricks.labs.mosaic.core.raster.operator.gdal

import com.databricks.labs.mosaic.core.raster.gdal.MosaicRasterGDAL
import org.gdal.gdal.{WarpOptions, gdal}

import java.nio.file.{Files, Paths}

/** GDALWarp is a wrapper for the GDAL Warp command. */
object GDALWarp {

    /**
      * Executes the GDAL Warp command.
      *
      * @param outputPath
      *   The output path of the warped file.
      * @param rasters
      *   The rasters to warp.
      * @param command
      *   The GDAL Warp command.
      * @return
      *   A MosaicRaster object.
      */
    def executeWarp(outputPath: String, rasters: Seq[MosaicRasterGDAL], command: String): MosaicRasterGDAL = {
        require(command.startsWith("gdalwarp"), "Not a valid GDAL Warp command.")
        // Test: gdal.ParseCommandLine(command)
        val effectiveCommand = OperatorOptions.appendOptions(command, rasters.head.getWriteOptions)
        val warpOptionsVec = OperatorOptions.parseOptions(effectiveCommand)
        val warpOptions = new WarpOptions(warpOptionsVec)
        val result = gdal.Warp(outputPath, rasters.map(_.getRaster).toArray, warpOptions)
        // Format will always be the same as the first raster
        val errorMsg = gdal.GetLastErrorMsg
        val size = Files.size(Paths.get(outputPath))
        val createInfo = Map(
          "path" -> outputPath,
          "parentPath" -> rasters.head.getParentPath,
          "driver" -> rasters.head.getWriteOptions.format,
          "last_command" -> effectiveCommand,
          "last_error" -> errorMsg,
          "all_parents" -> rasters.map(_.getParentPath).mkString(";")
        )
        rasters.head.copy(raster = result, createInfo = createInfo, memSize = size).flushCache()
    }

}
