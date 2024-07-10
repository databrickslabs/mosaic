package com.databricks.labs.mosaic.core.raster.operator.gdal

import com.databricks.labs.mosaic.{NO_PATH_STRING, RASTER_ALL_PARENTS_KEY, RASTER_DRIVER_KEY, RASTER_LAST_CMD_KEY, RASTER_LAST_ERR_KEY, RASTER_MEM_SIZE_KEY, RASTER_PARENT_PATH_KEY, RASTER_PATH_KEY}
import com.databricks.labs.mosaic.core.raster.gdal.RasterGDAL
import com.databricks.labs.mosaic.core.raster.io.RasterIO.flushAndDestroy
import com.databricks.labs.mosaic.functions.ExprConfig
import org.gdal.gdal.{WarpOptions, gdal}

import java.nio.file.{Files, Paths}
import scala.util.Try

/** GDALWarp is a wrapper for the GDAL Warp command. */
object GDALWarp {

    /**
      * Executes the GDAL Warp command.
      *
      * @param outputPath
      *   The output path of the warped file.
      * @param rasters
      *   The rasters to warp.
      * @param command
      *   The GDAL Warp command.
      * @param exprConfigOpt
      *   Option [[ExprConfig]]
      * @return
      *   A Raster object.
      */
    def executeWarp(outputPath: String, rasters: Seq[RasterGDAL], command: String, exprConfigOpt: Option[ExprConfig]): RasterGDAL = {
        require(command.startsWith("gdalwarp"), "Not a valid GDAL Warp command.")
        // Test: gdal.ParseCommandLine(command)
        val effectiveCommand = OperatorOptions.appendOptions(command, rasters.head.getWriteOptions)
        val warpOptionsVec = OperatorOptions.parseOptions(effectiveCommand)
        val warpOptions = new WarpOptions(warpOptionsVec)
        val warpResult = gdal.Warp(outputPath, rasters.map(_.withDatasetHydratedOpt().get).toArray, warpOptions)
        // Format will always be the same as the first tile
        val errorMsg = gdal.GetLastErrorMsg

//        if (errorMsg.nonEmpty) {
//            // scalastyle:off println
//            println(s"... GDALWarp (last_error) - '$errorMsg' for '$outputPath'")
//            // scalastyle:on println
//        }

        flushAndDestroy(warpResult)

        val size = Try(Files.size(Paths.get(outputPath))).getOrElse(-1L)
        val createInfo = Map(
          RASTER_PATH_KEY -> outputPath,
          RASTER_PARENT_PATH_KEY -> rasters.head.identifyPseudoPathOpt().getOrElse(NO_PATH_STRING),
          RASTER_DRIVER_KEY -> rasters.head.getWriteOptions.format,
          RASTER_MEM_SIZE_KEY -> size.toString,
          RASTER_LAST_CMD_KEY -> effectiveCommand,
          RASTER_LAST_ERR_KEY -> errorMsg,
          RASTER_ALL_PARENTS_KEY -> rasters.map(_.getRawParentPath).mkString(";")
        )

       RasterGDAL(createInfo, exprConfigOpt)
    }

}
