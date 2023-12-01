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
        val warpOptionsVec = OperatorOptions.parseOptions(command)
        val warpOptions = new WarpOptions(warpOptionsVec)
        val result = gdal.Warp(outputPath, rasters.map(_.getRaster).toArray, warpOptions)
        // TODO: Figure out multiple parents, should this be an array?
        // Format will always be the same as the first raster
        if (result == null) {
            throw new Exception(s"""
                                   |Warp failed.
                                   |Command: $command
                                   |Error: ${gdal.GetLastErrorMsg}
                                   |""".stripMargin)
        }
        val size = Files.size(Paths.get(outputPath))
        rasters.head
            .copy(
              raster = result,
              path = outputPath,
              memSize = size
            )
            .flushCache()
    }

}
