package com.databricks.labs.mosaic.datasource

import com.databricks.labs.mosaic.expressions.raster
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.ogr.ogr

object Utils {

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

    def getCleanPath(path: String, useZipPath: Boolean): String = {
        val cleanPath = path.replace("file:/", "/").replace("dbfs:/", "/dbfs/")
        if (useZipPath && cleanPath.endsWith(".zip")) {
            s"/vsizip/$cleanPath"
        } else {
            cleanPath
        }
    }

}
