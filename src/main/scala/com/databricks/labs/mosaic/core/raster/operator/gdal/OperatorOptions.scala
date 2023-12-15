package com.databricks.labs.mosaic.core.raster.operator.gdal

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

}
