package com.databricks.labs.mosaic.utils

import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.expressions.raster
import org.apache.hadoop.fs.FileStatus
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.unsafe.types.UTF8String

import java.util.Locale

object ReaderUtils {

    /**
     * Creates a Spark SQL row from a sequence of values.
     *
     * @param values
     *   sequence of values.
     * @return
     *   Spark SQL row.
     */
    def createRow(values: Seq[Any]): InternalRow = {
        InternalRow.fromSeq(
            values.map {
                case null           => null
                case b: Array[Byte] => b
                case v: Array[_]    => new GenericArrayData(v)
                case m: Map[_, _]   => raster.buildMapString(m.map { case (k, v) => (k.toString, v.toString) })
                case s: String      => UTF8String.fromString(s)
                case v              => v
            }
        )
    }

    def asTmpRaster(inPath: String, options: Map[String, String]): String = {
        if (options.getOrElse("readSubdataset", "false").toBoolean) {
            val readRaster = GDAL.raster(inPath, inPath)
            val subDatasets = readRaster.subdatasets
            if (subDatasets.isEmpty) {
                throw new RuntimeException(
                    s"Option 'readSubdataset' was set to 'true' but no subdatasets were found in $inPath"
                )
            }
            if (options.contains("subdatasetName")) {
                val subdatasetName = options("subdatasetName")
                if (!subDatasets.contains(subdatasetName)) {
                    throw new RuntimeException(s"Subdataset $subdatasetName not found in $inPath")
                }

                val subdatasetPath = PathUtils.createTmpFilePath(readRaster.getRasterFileExtension)
                readRaster.getSubdataset(subdatasetName).writeToPath(subdatasetPath)
                readRaster.destroy()
                subdatasetPath
            } else {
                throw new RuntimeException(
                    s"Option 'readSubdataset' was set to 'true' but 'subdatasetName' was not provided for $inPath}"
                )
            }
        } else {
            PathUtils.copyToTmpWithRetry(inPath, 5)
        }
    }

    /**
     * Indicates whether the file extension is allowed.
     * @param status
     *   File status.
     * @param options
     *   Reading options.
     * @return
     *   True if the file extension is allowed, false otherwise.
     */
    def isAllowedExtension(status: FileStatus, options: Map[String, String]): Boolean = {
        val allowedExtensions = options.getOrElse("extensions", "*").split(";").map(_.trim.toLowerCase(Locale.ROOT))
        val fileExtension = status.getPath.getName.toLowerCase(Locale.ROOT)
        allowedExtensions.contains("*") || allowedExtensions.exists(fileExtension.endsWith)
    }

}
