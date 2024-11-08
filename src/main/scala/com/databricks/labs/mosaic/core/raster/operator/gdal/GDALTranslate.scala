package com.databricks.labs.mosaic.core.raster.operator.gdal

import com.databricks.labs.mosaic.core.raster.gdal.{MosaicRasterGDAL, MosaicRasterWriteOptions}
import org.gdal.gdal.{TranslateOptions, gdal}

import java.nio.file.{Files, Paths}
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
      *   A MosaicRaster object.
      */
    def executeTranslate(
        outputPath: String,
        raster: MosaicRasterGDAL,
        command: String,
        writeOptions: MosaicRasterWriteOptions
    ): MosaicRasterGDAL = {
        require(command.startsWith("gdal_translate"), "Not a valid GDAL Translate command.")
        val effectiveCommand = OperatorOptions.appendOptions(command, writeOptions)
        val translateOptionsVec = OperatorOptions.parseOptions(effectiveCommand)
        val translateOptions = new TranslateOptions(translateOptionsVec)
        val result = gdal.Translate(outputPath, raster.getRaster, translateOptions)
        val errorMsg = gdal.GetLastErrorMsg
        val size = Try(Files.size(Paths.get(outputPath))).getOrElse(
            {
                val msg = "Error during GDAL translate operation: " +
                    s"file ${raster.getPath} could not be translated to $outputPath " +
                    s"with command '$effectiveCommand'. GDAL returned error: $errorMsg"
                throw new Exception(msg)
            }
        )
        val createInfo = Map(
          "path" -> outputPath,
          "parentPath" -> raster.getParentPath,
          "driver" -> writeOptions.format,
          "last_command" -> effectiveCommand,
          "last_error" -> errorMsg,
          "all_parents" -> raster.getParentPath
        )
        raster
            .copy(raster = result, createInfo = createInfo)
            .flushCache()
    }

}
