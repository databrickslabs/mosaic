package com.databricks.labs.mosaic.core.raster.operator.gdal

import com.databricks.labs.mosaic.core.raster.MosaicRaster
import com.databricks.labs.mosaic.core.raster.gdal_raster.MosaicRasterGDAL
import org.gdal.gdal.{TranslateOptions, gdal}

import java.nio.file.{Files, Paths}

object GDALTranslate {

    def executeTranslate(outputPath: String, isTemp: Boolean, raster: MosaicRaster, command: String): MosaicRaster = {
        val args = command.split(" ")
        if (args.head == "gdal_translate") {
            val translateOptionsVec = OperatorOptions.parseOptions(command)
            val translateOptions = new TranslateOptions(translateOptionsVec)
            val result = gdal.Translate(outputPath, raster.getRaster, translateOptions)
            val size = Files.size(Paths.get(outputPath))
            val mosaicRaster = MosaicRasterGDAL(result, outputPath, isTemp, raster.getParentPath, raster.getDriversShortName, size)
            mosaicRaster.flushCache()
        } else {
            throw new Exception("Not a valid GDAL Translate command.")
        }
    }

}
