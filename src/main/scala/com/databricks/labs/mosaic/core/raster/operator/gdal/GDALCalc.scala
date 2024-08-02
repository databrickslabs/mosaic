package com.databricks.labs.mosaic.core.raster.operator.gdal

import com.databricks.labs.mosaic.{RASTER_ALL_PARENTS_KEY, RASTER_DRIVER_KEY, RASTER_FULL_ERR_KEY, RASTER_LAST_CMD_KEY, RASTER_LAST_ERR_KEY, RASTER_PARENT_PATH_KEY, RASTER_PATH_KEY}
import com.databricks.labs.mosaic.core.raster.gdal.{RasterGDAL, RasterWriteOptions}
import com.databricks.labs.mosaic.functions.ExprConfig
import com.databricks.labs.mosaic.utils.SysUtils
import org.gdal.gdal.gdal

import scala.util.Try

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
      * @param exprConfigOpt
      *   Option [[ExprConfig]]
      * @return
      *   Returns the result as a [[RasterGDAL]].
      */
    def executeCalc(gdalCalcCommand: String, resultPath: String, exprConfigOpt: Option[ExprConfig]): RasterGDAL = {
        require(gdalCalcCommand.startsWith("gdal_calc"), "Not a valid GDAL Calc command.")
        val effectiveCommand = OperatorOptions.appendOptions(gdalCalcCommand, RasterWriteOptions.GTiff)

        Try {
            val toRun = effectiveCommand.replace("gdal_calc", gdal_calc)
            val commandRes = SysUtils.runCommand(s"python3 $toRun")
            val errorMsg = gdal.GetLastErrorMsg

            //        if (errorMsg.nonEmpty) {
            //            // scalastyle:off println
            //            println(s"... GDALCalc (last_error) - '$errorMsg' for '$resultPath'")
            //            // scalastyle:on println
            //        }

            val createInfo = Map(
                RASTER_PATH_KEY -> resultPath,
                RASTER_PARENT_PATH_KEY -> resultPath,
                RASTER_DRIVER_KEY -> "GTiff",
                RASTER_LAST_CMD_KEY -> effectiveCommand,
                RASTER_LAST_ERR_KEY -> errorMsg,
                RASTER_ALL_PARENTS_KEY -> resultPath,
                RASTER_FULL_ERR_KEY -> s"""
                                          |GDAL Calc command failed:
                                          |GDAL err:
                                          |$errorMsg
                                          |STDOUT:
                                          |${commandRes._2}
                                          |STDERR:
                                          |${commandRes._3}
                                          |""".stripMargin
            )
            RasterGDAL(createInfo, exprConfigOpt)
        }.getOrElse {
            val result = RasterGDAL() // <- empty raster
            result.updateCreateInfoLastCmd(effectiveCommand)
            result.updateCreateInfoError("GDAL Calc command threw exception")
            result
        }
    }

}
