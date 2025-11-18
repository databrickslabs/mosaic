package com.databricks.labs.gbx.rasterx.operator

import com.databricks.labs.gbx.util.SysUtils
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants.GA_ReadOnly

import java.nio.file.{Files, Paths}
import scala.util.Try

/** GDALCalc is a helper object for executing GDAL Calc commands. */
object GDALCalc {

    private val gdal_calc: String = {
        val calcPath = SysUtils.runCommand(Seq("which", "gdal_calc.py"))._1.split("\n").headOption.getOrElse("")
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
      *   Returns the result as a [[Dataset]].
      */
    def executeCalc(
        gdalCalcCommand: String,
        resultPath: String,
        options: Map[String, String],
        ds: Dataset
    ): (Dataset, Map[String, String]) = {
        require(gdalCalcCommand.startsWith("gdal_calc"), "Not a valid GDAL Calc command.")
        val effectiveCommand = OperatorOptions.appendOptions(gdalCalcCommand, options, ds)
        val toRun = effectiveCommand.replace("gdal_calc", gdal_calc)
        val commandRes = SysUtils.runCommand(Seq("python3", "-u") ++ toRun.split(" ").filterNot(_.isEmpty).toSeq)
        val errorMsg = gdal.GetLastErrorMsg
        val result = gdal.Open(resultPath, GA_ReadOnly)
        val size = Try {
            if (resultPath.startsWith("/vsimem/")) gdal.GetMemFileBuffer(resultPath).length
            else Files.size(Paths.get(resultPath))
        }.getOrElse(-1L)
        // noinspection DuplicatedCode
        // TODO: make errors better, this is quite aggressive
        val newOptions = Map(
          "path" -> resultPath,
          "parentPath" -> resultPath,
          "driver" -> "GTiff",
          "last_command" -> effectiveCommand,
          "last_error" -> errorMsg,
          "all_parents" -> resultPath,
          "size" -> size.toString,
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
        (result, newOptions)
    }

}
