package com.databricks.labs.gbx.rasterx.operator

import org.gdal.gdal.{Dataset, WarpOptions, gdal}

import java.nio.file.{Files, Paths}
import scala.util.Try

/** GDALWarp is a wrapper for the GDAL Warp command. */
object GDALWarp {

    private def addSrcSRS(options: java.util.Vector[String], ds: Dataset): java.util.Vector[String] = {
        val srs = ds.GetSpatialRef()
        if (srs == null) {
            val srsVec = new java.util.Vector[String]()
            srsVec.add("-s_srs")
            srsVec.add("EPSG:4326")
            srsVec.addAll(options)
            srsVec
        } else {
            options
        }
    }

    /**
      * Executes the GDAL Warp command.
      *
      * @param outputPath
      *   The output path of the warped file.
      * @param dss
      *   The rasters to warp.
      * @param command
      *   The GDAL Warp command.
      * @return
      *   A Raster object.
      */
    def executeWarp(
        outputPath: String,
        dss: Array[Dataset],
        options: Map[String, String],
        command: String
    ): (Dataset, Map[String, String]) = {
        require(command.startsWith("gdalwarp"), "Not a valid GDAL Warp command.")

        val effectiveCommand = OperatorOptions.appendOptions(command, options, dss.head)
        val warpOptionsVec = OperatorOptions.parseOptions(effectiveCommand)
        val adjustedVec = addSrcSRS(warpOptionsVec, dss.head)
        val warpOptions = new WarpOptions(adjustedVec)
        val result = gdal.Warp(outputPath, dss, warpOptions)
        // Format will always be the same as the first raster
        val errorMsg = gdal.GetLastErrorMsg
        val size = Try(Files.size(Paths.get(outputPath))).getOrElse(-1)
        val newOptions = Map(
          "path" -> outputPath,
          "parentPath" -> options.getOrElse("path", ""),
          "driver" -> options.getOrElse("driver", dss.head.GetDriver().getShortName),
          "last_command" -> effectiveCommand,
          "last_error" -> errorMsg,
          "all_parents" -> dss.map(_.GetDescription()).mkString(";"), // TODO: this should be a union of all parents of all dss
          "size" -> size.toString,
          "format" -> dss.head.GetDriver().getShortName,
          "compression" -> options.getOrElse("compression", "ZSTD"),
          "isZipped" -> "false",
          "isSubset" -> "false"
        )
        Try(result.FlushCache())
        (result, newOptions)
    }

}
