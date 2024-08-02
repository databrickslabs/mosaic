package com.databricks.labs.mosaic.core.raster.operator.gdal

import com.databricks.labs.mosaic.{
    NO_PATH_STRING,
    RASTER_ALL_PARENTS_KEY,
    RASTER_BAND_INDEX_KEY,
    RASTER_DRIVER_KEY,
    RASTER_LAST_CMD_KEY,
    RASTER_LAST_ERR_KEY,
    RASTER_PARENT_PATH_KEY,
    RASTER_PATH_KEY,
    RASTER_SUBDATASET_NAME_KEY
}
import com.databricks.labs.mosaic.core.raster.gdal.{RasterGDAL, RasterWriteOptions}
import com.databricks.labs.mosaic.core.raster.io.RasterIO.flushAndDestroy
import com.databricks.labs.mosaic.functions.ExprConfig
import org.gdal.gdal.{TranslateOptions, gdal}

import scala.util.Try


/** GDALTranslate is a wrapper for the GDAL Translate command. */
object GDALTranslate {

    /**
      * Executes the GDAL Translate command.
      *
      * @param outputPath
      *   The output path of the translated file.
      * @param raster
      *   The tile to translate.
      * @param command
      *   The GDAL Translate command.
      * @writeOptions
      *    [[RasterWriteOptions]]
      * @exprConfigOpt
      *   Option [[ExprConfig]]
      * @return
      *   A MosaicRaster object.
      */
    def executeTranslate(
        outputPath: String,
        raster: RasterGDAL,
        command: String,
        writeOptions: RasterWriteOptions,
        exprConfigOpt: Option[ExprConfig]
    ): RasterGDAL = {
        // scalastyle:off println
        require(command.startsWith("gdal_translate"), "Not a valid GDAL Translate command.")
        val effectiveCommand = OperatorOptions.appendOptions(command, writeOptions)
        //println(s"GDALTranslate - is raster hydrated? ${raster.isDatasetHydrated}")
        //println(s"GDALTranslate - createInfo? ${raster.getCreateInfo}")

        Try {
            val translateOptionsVec = OperatorOptions.parseOptions(effectiveCommand)
            val translateOptions = new TranslateOptions(translateOptionsVec)
            val transResult = gdal.Translate(outputPath, raster.withDatasetHydratedOpt().get, translateOptions)
            val errorMsg = gdal.GetLastErrorMsg

            //        if (errorMsg.nonEmpty) {
            //            println(s"... GDALTranslate (last_error) - '$errorMsg' for '$outputPath'")
            //        }

            flushAndDestroy(transResult)

            RasterGDAL(
                Map(
                    RASTER_PATH_KEY -> outputPath,
                    RASTER_PARENT_PATH_KEY -> raster.identifyPseudoPathOpt().getOrElse(NO_PATH_STRING),
                    RASTER_DRIVER_KEY -> writeOptions.format,
                    RASTER_SUBDATASET_NAME_KEY -> raster.getCreateInfoSubdatasetNameOpt.getOrElse(""),
                    RASTER_BAND_INDEX_KEY -> raster.getCreateInfoBandIndexOpt.getOrElse(-1).toString,
                    RASTER_LAST_CMD_KEY -> effectiveCommand,
                    RASTER_LAST_ERR_KEY -> errorMsg,
                    RASTER_ALL_PARENTS_KEY -> raster.getRawParentPath
                ),
                exprConfigOpt
            )
        }.getOrElse {
            val result = RasterGDAL() // <- empty raster
            result.updateCreateInfoLastCmd(effectiveCommand)
            result.updateCreateInfoError("GDAL Translate command threw exception")
            result
        }
        // scalastyle:on println
    }

}
