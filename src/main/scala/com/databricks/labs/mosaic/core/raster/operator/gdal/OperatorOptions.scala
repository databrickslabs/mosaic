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
        val args = parseAndDeduplicate(command)
        val optionsVec = new java.util.Vector[String]()
        args.foreach(optionsVec.add)
        optionsVec
    }

    def parseAndDeduplicate(args: String): List[String] = {
        // Split the input string into an array by whitespace
        val parts = args.split("\\s+")

        // Mutable structures to track unique flags and allow duplicate prefixes
        val seenFlags = scala.collection.mutable.Map[String, List[String]]()
        val preservedMultipleFlags = scala.collection.mutable.ListBuffer[String]()

        val flagRegex = """^-[a-zA-Z]""".r

        // Process the arguments
        var i = 0
        while (i < parts.length) {
            val flag = parts(i)
            if (flag.startsWith("-")) {
                // Slice the array for all associated values
                val values = parts.slice(i + 1, parts.length).takeWhile(v => flagRegex.findFirstIn(v).isEmpty)
                if (flag.startsWith("-co") || flag.startsWith("-wo")) {
                    // Allow multiple instances of these (preserve all values)
                    preservedMultipleFlags += flag
                    preservedMultipleFlags ++= values
                } else {
                    // Deduplicate by keeping only the latest values
                    seenFlags(flag) = values.toList
                }
                i += values.length // Skip over the values
            }
            i += 1 // Move to the next flag
        }

        // Combine the deduplicated flags and preserved multiple flags
        val deduplicatedArgs = seenFlags.flatMap {
            case (flag, values) =>
                if (values.isEmpty) List(flag) // Flags without values
                else flag +: values // Include flag and its associated values
        }

        // Return the final deduplicated and ordered list
        (deduplicatedArgs ++ preservedMultipleFlags).toList
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
