package com.databricks.labs.mosaic.core.raster.operator.gdal

import com.databricks.labs.mosaic.core.raster.gdal.MosaicRasterGDAL
import org.gdal.gdal.{InfoOptions, gdal}

/** GDALBuildVRT is a wrapper for the GDAL BuildVRT command. */
object GDALInfo {

    /**
      * Executes the GDAL BuildVRT command. For flags check the way gdalinfo.py
      * script is called, InfoOptions expects a collection of same flags.
      *
      * @param raster
      *   The raster to get info from.
      * @param command
      *   The GDAL Info command.
      * @return
      *   A result json string.
      */
    def executeInfo(raster: MosaicRasterGDAL, command: String): String = {
        require(command.startsWith("gdalinfo"), "Not a valid GDAL Info command.")

        val infoOptionsVec = OperatorOptions.parseOptions(command)
        val infoOptions = new InfoOptions(infoOptionsVec)
        val gdalInfo = gdal.GDALInfo(raster.getRaster, infoOptions)

        if (gdalInfo == null) {
            s"""
               |GDAL Info failed.
               |Command: $command
               |Error: ${gdal.GetLastErrorMsg}
               |""".stripMargin
        } else {
            gdalInfo
        }
    }

}
