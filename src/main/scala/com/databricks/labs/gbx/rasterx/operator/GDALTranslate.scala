package com.databricks.labs.gbx.rasterx.operator

import org.gdal.gdal.{Dataset, TranslateOptions, gdal}

import java.nio.file.{Files, Paths}
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.util.Try

/** GDALTranslate is a wrapper for the GDAL Translate command. */
object GDALTranslate {

    /**
      * Executes the GDAL Translate command.
      *
      * @param outputPath
      *   The output path of the translated file.
      * @param raster
      *   The raster to translate.
      * @param command
      *   The GDAL Translate command.
      * @return
      *   A Raster object.
      */
    def executeTranslate(
        outputPath: String,
        raster: Dataset,
        command: String,
        options: Map[String, String]
    ): (Dataset, Map[String, String]) = {
        require(command.startsWith("gdal_translate"), "Not a valid GDAL Translate command.")
        val effectiveCommand = OperatorOptions.appendOptions(command, options, raster)
        val translateOptionsVec = OperatorOptions.parseOptions(effectiveCommand)
        val translateOptions = new TranslateOptions(translateOptionsVec)
        val result = gdal.Translate(outputPath, raster, translateOptions)
        val errorMsg = gdal.GetLastErrorMsg
        val sourcePath = raster.GetFileList().asScala.headOption.map(_.toString).getOrElse("unknown source path")
        val size = Try(Files.size(Paths.get(outputPath))).getOrElse(-1L)
        // TODO: build a JNA bridge for VSI mem estimate
        val newOptions = Map(
          "path" -> outputPath,
          "sourcePath" -> sourcePath,
          "driver" -> raster.GetDriver().getShortName,
          "last_command" -> effectiveCommand,
          "last_error" -> errorMsg,
          "all_parents" -> s"$sourcePath;${options.getOrElse("all_parents", "")}",
          "size" -> size.toString, // For in memory we always return -1
          "format" -> raster.GetDriver().getShortName,
          "compression" -> options.getOrElse("compression", "ZSTD"),
          "isZipped" -> "false",
          "isSubset" -> "false"
        )
        Try(result.FlushCache())
        (result, newOptions)
    }

}
