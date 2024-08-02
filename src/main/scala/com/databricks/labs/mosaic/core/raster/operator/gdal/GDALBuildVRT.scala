package com.databricks.labs.mosaic.core.raster.operator.gdal

import com.databricks.labs.mosaic.{
    RASTER_ALL_PARENTS_KEY,
    RASTER_DRIVER_KEY,
    RASTER_LAST_CMD_KEY,
    RASTER_LAST_ERR_KEY,
    RASTER_PARENT_PATH_KEY,
    RASTER_PATH_KEY
}
import com.databricks.labs.mosaic.core.raster.gdal.{RasterGDAL, RasterWriteOptions}
import com.databricks.labs.mosaic.core.raster.io.RasterIO.flushAndDestroy
import com.databricks.labs.mosaic.functions.ExprConfig
import org.gdal.gdal.{BuildVRTOptions, gdal}

/** GDALBuildVRT is a wrapper for the GDAL BuildVRT command. */
object GDALBuildVRT {

    /**
      * Executes the GDAL BuildVRT command.
      *
      * @param outputPath
      *   The output path of the VRT file.
      * @param rasters
      *   The rasters to build the VRT from.
      * @param command
      *   The GDAL BuildVRT command.
      * @param exprConfigOpt
      *   Option [[ExprConfig]]
      * @return
      *   A MosaicRaster object.
      */
    def executeVRT(outputPath: String, rasters: Seq[RasterGDAL], command: String, exprConfigOpt: Option[ExprConfig]): RasterGDAL = {
        require(command.startsWith("gdalbuildvrt"), "Not a valid GDAL Build VRT command.")
        val effectiveCommand = OperatorOptions.appendOptions(command, RasterWriteOptions.VRT)
        val vrtOptionsVec = OperatorOptions.parseOptions(effectiveCommand)
        val vrtOptions = new BuildVRTOptions(vrtOptionsVec)
        // filter empty rasters
        val vrtResult = gdal.BuildVRT(
            outputPath,
            rasters
                .filter(!_.isEmptyRasterGDAL)
                .filter(!_.isEmpty)
                .map(_.withDatasetHydratedOpt().get)
                .toArray,
            vrtOptions
        )
        flushAndDestroy(vrtResult)

        val errorMsg = gdal.GetLastErrorMsg
        if (errorMsg.nonEmpty) {
            // scalastyle:off println
            //println(s"... GDALBuildVRT (last_error) - '$errorMsg' for '$outputPath'")
            // scalastyle:on println
            val result = RasterGDAL()
            result.updateCreateInfoLastCmd(effectiveCommand)
            result.updateCreateInfoError(errorMsg)

            result
        } else {
            val createInfo = Map(
                RASTER_PATH_KEY -> outputPath,
                RASTER_PARENT_PATH_KEY -> rasters.head.getRawParentPath,
                RASTER_DRIVER_KEY -> "VRT",
                RASTER_LAST_CMD_KEY -> effectiveCommand,
                RASTER_LAST_ERR_KEY -> errorMsg,
                RASTER_ALL_PARENTS_KEY -> rasters.map(_.getRawParentPath).mkString(";")
            )

            RasterGDAL(createInfo, exprConfigOpt)
        }
    }

}
