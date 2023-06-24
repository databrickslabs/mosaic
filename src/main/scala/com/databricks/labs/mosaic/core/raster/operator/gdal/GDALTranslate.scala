package com.databricks.labs.mosaic.core.raster.operator.gdal

import com.databricks.labs.mosaic.core.raster.MosaicRaster
import com.databricks.labs.mosaic.core.raster.gdal_raster.{MosaicRasterGDAL, RasterCleaner}
import org.gdal.gdal.{TranslateOptions, gdal}

object GDALTranslate {

    def executeTranslate(outputPath: String, raster: MosaicRaster, command: String): MosaicRaster = {
        val args = command.split(" ")
        if (args.head == "gdal_translate") {
            val translateOptionsVec = OperatorOptions.parseOptions(command)
            val translateOptions = new TranslateOptions(translateOptionsVec)
            val result = gdal.Translate(outputPath, raster.getRaster, translateOptions)
            MosaicRasterGDAL(result, outputPath)
        } else {
            throw new Exception("Not a valid GDAL Translate command.")
        }
    }

}
