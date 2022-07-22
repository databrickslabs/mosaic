package com.databricks.labs.mosaic.core.geometry

import com.databricks.labs.mosaic.core.types._
import org.apache.spark.sql.types._

object GeometryFormat {

    def getDefaultFormat(outputDataType: DataType): String = {
        outputDataType match {
            case BinaryType           => "WKB"
            case StringType           => "WKT"
            case HexType              => "HEX"
            case JSONType             => "JSONOBJECT"
            case InternalGeometryType => "COORDS"
            case _                    => throw new Error(s"Unsupported data type ${outputDataType.typeName}.")
        }
    }
}
