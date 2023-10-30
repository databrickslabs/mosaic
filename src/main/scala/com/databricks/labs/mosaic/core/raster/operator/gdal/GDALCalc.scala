package com.databricks.labs.mosaic.core.raster.operator.gdal

import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.core.raster.gdal.MosaicRasterGDAL

/** GDALCalc is a helper object for executing GDAL Calc commands. */
object GDALCalc {

    val gdal_calc = "/usr/lib/python3/dist-packages/osgeo_utils/gdal_calc.py"

    /**
      * Executes the GDAL Calc command.
      * @param gdalCalcCommand
      *   The GDAL Calc command to execute.
      * @param resultPath
      *   The path to the result.
      * @return
      *   Returns the result as a [[MosaicRasterGDAL]].
      */
    def executeCalc(gdalCalcCommand: String, resultPath: String): MosaicRasterGDAL = {
        require(gdalCalcCommand.startsWith("gdal_calc"), "Not a valid GDAL Calc command.")
        import sys.process._
        val toRun = gdalCalcCommand.replace("gdal_calc", gdal_calc)
        s"sudo python3 $toRun".!!
        val result = GDAL.raster(resultPath, resultPath)
        result
    }

}
