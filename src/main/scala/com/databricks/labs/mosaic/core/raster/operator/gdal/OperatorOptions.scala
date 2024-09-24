package com.databricks.labs.mosaic.core.raster.operator.gdal

import com.databricks.labs.mosaic.core.raster.gdal.MosaicRasterWriteOptions

/** OperatorOptions is a helper object for parsing GDAL command options. */
object OperatorOptions {

    /**
      * Parses the options from a GDAL command.
      *
      * @param command
      *   The GDAL command.
      * @return
      *   A vector of options.
      */
    def parseOptions(command: String): java.util.Vector[String] = {
        val args = command.split(" ")
        val optionsVec = new java.util.Vector[String]()
        args.drop(1).foreach(optionsVec.add)
        optionsVec
    }

    /**
      * Add default options to the command. Extract the compression from the
      * raster and append it to the command. This operation does not change the
      * output format. For changing the output format, use RST_ToFormat.
      *
      * @param command
      *   The command to append options to.
      * @param writeOptions
      *   The write options to append. Note that not all available options are
      *   actually appended. At this point it is up to the bellow logic to
      *   decide what is supported and for which format.
      * @return
      */
    def appendOptions(command: String, writeOptions: MosaicRasterWriteOptions): String = {
        val compression = writeOptions.compression
        if (command.startsWith("gdal_calc")) {
            writeOptions.format match {
                case f @ "GTiff" => command + s" --format $f --co TILED=YES --co COMPRESS=$compression"
                case f @ "COG"   => command + s" --format $f --co TILED=YES --co COMPRESS=$compression"
                case f @ _       => command + s" --format $f --co COMPRESS=$compression"
            }
        } else {
            writeOptions.format match {
                case f @ "GTiff"                              => command + s" -of $f -co TILED=YES -co COMPRESS=$compression"
                case f @ "COG"                                => command + s" -of $f -co TILED=YES -co COMPRESS=$compression"
                case "VRT"                                    => command
                case f @ "Zarr" if writeOptions.missingGeoRef =>
                    command + s" -of $f -co COMPRESS=$compression -to SRC_METHOD=NO_GEOTRANSFORM"
                case f @ _                                    => command + s" -of $f -co COMPRESS=$compression"
            }
        }
    }

}
