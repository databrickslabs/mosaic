package com.databricks.labs.mosaic.core.raster.operator.gdal

import com.databricks.labs.mosaic.core.raster.gdal.MosaicRasterGDAL
import org.gdal.gdal.{TranslateOptions, gdal}

import java.nio.file.{Files, Paths}

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
    def executeTranslate(outputPath: String, raster: MosaicRasterGDAL, command: String): MosaicRasterGDAL = {
        require(command.startsWith("gdal_translate"), "Not a valid GDAL Translate command.")
        val translateOptionsVec = OperatorOptions.parseOptions(command)
        val translateOptions = new TranslateOptions(translateOptionsVec)
        val result = gdal.Translate(outputPath, raster.getRaster, translateOptions)
        if (result == null) {
            throw new Exception(
                s"""
                   |Translate failed.
                   |Command: $command
                   |Error: ${gdal.GetLastErrorMsg}
                   |""".stripMargin)
        }
        val size = Files.size(Paths.get(outputPath))
        raster.copy(raster = result, path = outputPath, memSize = size).flushCache()
    }

}
