package com.databricks.labs.mosaic.expressions.geometry.base

import com.databricks.labs.mosaic.core.types._
import com.databricks.labs.mosaic.sql.MosaicSQLExceptions
import org.apache.spark.sql.types._

trait RequiresCRS {

    val encodings = List("COORDS", "GEOJSON")

    def getEncoding(dataType: DataType): String =
        dataType match {
            case StringType           => "WKT"
            case BinaryType           => "WKB"
            case HexType              => "HEX"
            case JSONType             => "GEOJSON"
            case InternalGeometryType => "COORDS"
            case _                    => throw new Error("Format not supported!")
        }

    def checkEncoding(dataType: DataType): Unit = {
        val inputTypeEncoding = getEncoding(dataType)
        if (!encodings.contains(inputTypeEncoding)) {
            throw MosaicSQLExceptions.GeometryEncodingNotSupported(encodings, inputTypeEncoding)
        }
    }

}
