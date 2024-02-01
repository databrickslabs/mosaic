package com.databricks.labs.mosaic.core.raster.operator.gdal

import com.databricks.labs.mosaic.core.raster.gdal.{MosaicRasterGDAL, MosaicRasterWriteOptions}
import org.gdal.gdal.{BuildVRTOptions, gdal}

/** GDALBuildVRT is a wrapper for the GDAL BuildVRT command. */
object GDALBuildVRT {

    /**
      * Executes the GDAL BuildVRT command.
      *
      * @param outputPath
      *   The output path of the VRT file.
      * @param rasters
      *   The rasters to build the VRT from.
      * @param command
      *   The GDAL BuildVRT command.
      * @return
      *   A MosaicRaster object.
      */
    def executeVRT(outputPath: String, rasters: Seq[MosaicRasterGDAL], command: String): MosaicRasterGDAL = {
        require(command.startsWith("gdalbuildvrt"), "Not a valid GDAL Build VRT command.")
        val effectiveCommand = OperatorOptions.appendOptions(command, MosaicRasterWriteOptions.VRT)
        val vrtOptionsVec = OperatorOptions.parseOptions(effectiveCommand)
        val vrtOptions = new BuildVRTOptions(vrtOptionsVec)
        val result = gdal.BuildVRT(outputPath, rasters.map(_.getRaster).toArray, vrtOptions)
        if (result == null) {
            throw new Exception(s"""
                                   |Build VRT failed.
                                   |Command: $effectiveCommand
                                   |Error: ${gdal.GetLastErrorMsg}
                                   |""".stripMargin)
        }
        // TODO: Figure out multiple parents, should this be an array?
        // VRT files are just meta files, mem size doesnt make much sense so we keep -1
        MosaicRasterGDAL(result, outputPath, rasters.head.getParentPath, "VRT", -1).flushCache()
    }

}
