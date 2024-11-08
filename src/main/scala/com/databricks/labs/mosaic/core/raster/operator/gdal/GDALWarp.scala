package com.databricks.labs.mosaic.core.raster.operator.gdal

import com.databricks.labs.mosaic.core.raster.gdal.MosaicRasterGDAL
import org.gdal.gdal.{Dataset, WarpOptions, gdal}
import org.gdal.gdalconst.gdalconstConstants

import java.nio.file.{Files, Paths}
import scala.sys.process._
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
      * @return
      *   A MosaicRaster object.
      */
    def executeWarp(outputPath: String, rasters: Seq[MosaicRasterGDAL], command: String): MosaicRasterGDAL = {
        require(command.startsWith("gdalwarp"), "Not a valid GDAL Warp command.")

        val effectiveCommand = OperatorOptions.appendOptions(command, rasters.head.getWriteOptions)
        val warpOptionsVec = OperatorOptions.parseOptions(effectiveCommand)
        val warpOptions = new WarpOptions(warpOptionsVec)
        val result = gdal.Warp(outputPath, rasters.map(_.getRaster).toArray, warpOptions)
        // Format will always be the same as the first raster
        val errorMsg = gdal.GetLastErrorMsg
        val size = Try(Files.size(Paths.get(outputPath))).getOrElse(
            {
                val msg = "Error during GDAL warp operation: " +
                    s"file(s) \\n '${rasters.map(_.getPath).mkString("\\n")}' could not be reprojected to $outputPath " +
                    s"with command '$effectiveCommand'. GDAL returned error: $errorMsg"
                throw new Exception(msg)
            }
        )
        val createInfo = Map(
          "path" -> outputPath,
          "parentPath" -> rasters.head.getParentPath,
          "driver" -> rasters.head.getWriteOptions.format,
          "last_command" -> effectiveCommand,
          "last_error" -> errorMsg,
          "all_parents" -> rasters.map(_.getParentPath).mkString(";")
        )
        rasters.head.copy(raster = result, createInfo = createInfo).flushCache()
    }

    /**
        * Executes the GDAL Warp command on a single raster in a thread-safe manner.
        * @param inPath
        *  The path of the input raster.
        * @param outputPath
        * The output path of the warped file.
        * @param effectiveCommand
        * The effective GDAL Warp command, e.g. `gdalwarp -d_srs EPSG:4326`.
        * @return
        * A Dataset object.
     **/
    private def warpSingleRasterWithGDALApplication(inPath: String, outputPath: String, effectiveCommand: String): Dataset = this.synchronized {
        val executedCommand = s"$effectiveCommand $inPath $outputPath"
        val commandOutput = executedCommand.!!
        gdal.Open(outputPath, gdalconstConstants.GA_ReadOnly)
    }

}
