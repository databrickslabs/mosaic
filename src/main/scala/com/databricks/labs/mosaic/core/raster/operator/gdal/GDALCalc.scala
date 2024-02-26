package com.databricks.labs.mosaic.core.raster.operator.gdal

import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.core.raster.gdal.{MosaicRasterGDAL, MosaicRasterWriteOptions}
import com.databricks.labs.mosaic.utils.SysUtils
import org.gdal.gdal.gdal

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
        val effectiveCommand = OperatorOptions.appendOptions(gdalCalcCommand, MosaicRasterWriteOptions.GTiff)
        val toRun = effectiveCommand.replace("gdal_calc", gdal_calc)
        val commandRes = SysUtils.runCommand(s"python3 $toRun")
        val errorMsg = gdal.GetLastErrorMsg
        val result = GDAL.raster(resultPath, resultPath)
        val createInfo = Map(
          "path" -> resultPath,
          "parentPath" -> resultPath,
          "driver" -> "GTiff",
          "last_command" -> effectiveCommand,
          "last_error" -> errorMsg,
          "all_parents" -> resultPath,
          "full_error" -> s"""
                             |GDAL Calc command failed:
                             |GDAL err:
                             |$errorMsg
                             |STDOUT:
                             |${commandRes._2}
                             |STDERR:
                             |${commandRes._3}
                             |""".stripMargin
        )
        result.copy(createInfo = createInfo)
    }

}
