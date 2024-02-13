package com.databricks.labs.mosaic.core.raster.operator.gdal

import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.core.raster.gdal.MosaicRasterGDAL
import com.databricks.labs.mosaic.utils.SysUtils

/** GDALCalc is a helper object for executing GDAL Calc commands. */
object GDALCalc {

    val gdal_calc: String = {
        val calcPath = SysUtils.runCommand("which gdal_calc.py")._1.split("\n").headOption.getOrElse("")
        if (calcPath.isEmpty) {
            throw new RuntimeException("Could not find gdal_calc.py.")
        }
        if (calcPath == "ERROR") {
            "/usr/lib/python3/dist-packages/osgeo_utils/gdal_calc.py"
        } else {
            calcPath
        }
    }

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
        val toRun = gdalCalcCommand.replace("gdal_calc", gdal_calc)
        val commandRes = SysUtils.runCommand(s"python3 $toRun")
        if (commandRes._1 == "ERROR") {
            throw new RuntimeException(s"""
                                          |GDAL Calc command failed:
                                          |STDOUT:
                                          |${commandRes._2}
                                          |STDERR:
                                          |${commandRes._3}
                                          |""".stripMargin)
        }
        val result = GDAL.raster(resultPath, resultPath)
        result
    }

}
