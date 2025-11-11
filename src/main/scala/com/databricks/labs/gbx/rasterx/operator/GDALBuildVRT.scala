package com.databricks.labs.gbx.rasterx.operator

import org.gdal.gdal.{BuildVRTOptions, Dataset, gdal}

import scala.util.Try

/** GDALBuildVRT is a wrapper for the GDAL BuildVRT command. */
object GDALBuildVRT {

    /**
      * Executes the GDAL BuildVRT command.
      *
      * @param outputPath
      *   The output path of the VRT file.
      * @param dss
      *   The rasters to build the VRT from.
      * @param command
      *   The GDAL BuildVRT command.
      * @return
      *   A Raster object.
      */
    def executeVRT(outputPath: String, dss: Array[Dataset], options: Map[String, String], command: String): (Dataset, Map[String, String]) = {
        require(command.startsWith("gdalbuildvrt"), "Not a valid GDAL Build VRT command.")
        val effectiveCommand = OperatorOptions.appendOptions(command, options, dss.head)
        val vrtOptionsVec = OperatorOptions.parseOptions(effectiveCommand)
        val vrtOptions = new BuildVRTOptions(vrtOptionsVec)
        val result = gdal.BuildVRT(outputPath, dss, vrtOptions)
        val errorMsg = gdal.GetLastErrorMsg
        // Assuming 8 bytes per pixel for double type
        // this may be a bit wasteful if the raster is not double type,
        // VRTs are just config files so this is best effort approximate
        val size = Try(result.getRasterXSize * result.getRasterYSize * result.getRasterCount * 8).getOrElse(-1L)
        val newOptions = Map(
          "path" -> outputPath,
          "parentPath" -> options.getOrElse("parentPath", dss.head.GetDescription()),
          "driver" -> "VRT",
          "last_command" -> effectiveCommand,
          "last_error" -> errorMsg,
          "size" -> size.toString,
          "all_parents" -> dss.map(_.GetDescription).mkString(";"),
          "compression" -> options.getOrElse("compression", "ZSTD"),
          "isZipped" -> "false",
          "isSubset" -> "false"
        )
        Try(result.FlushCache())
        (result, newOptions)
    }

}
