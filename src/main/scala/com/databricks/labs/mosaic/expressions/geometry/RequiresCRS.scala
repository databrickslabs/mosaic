package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.types.{HexType, InternalGeometryType, JSONType}
import com.databricks.labs.mosaic.sql.MosaicSQLExceptions

import org.apache.spark.sql.types.{BinaryType, DataType, StringType}

trait RequiresCRS {

    val encodings = List("COORDS", "GEOJSON")

    def getEncoding(dataType: DataType): String =
        dataType match {
            case StringType           => "WKT"
            case BinaryType           => "WKB"
            case HexType              => "HEX"
            case JSONType             => "GEOJSON"
            case InternalGeometryType => "COORDS"
            case _                    => ???
        }

    def checkEncoding(dataType: DataType): Unit = {
        val inputTypeEncoding = getEncoding(dataType)
        if (!encodings.contains(inputTypeEncoding)) {
            throw MosaicSQLExceptions.GeometryEncodingNotSupported(encodings, inputTypeEncoding)
        }
    }

}
