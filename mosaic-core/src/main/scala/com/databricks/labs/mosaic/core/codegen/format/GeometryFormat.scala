package com.databricks.labs.mosaic.core.codegen.format

import com.databricks.labs.mosaic.core.types._
import org.apache.spark.sql.types._

/** Utility object for handling default formats for data types */
object GeometryFormat {

  /**
   * Get the default format for a given data type.
   *
   * @param outputDataType The data type to get the default format for.
   * @return The default format for the given data type.
   */
  def getDefaultFormat(outputDataType: DataType): String = {
    outputDataType match {
      case BinaryType => "WKB"
      case StringType => "WKT"
      case HexType => "HEX"
      case GeoJSONType => "GEOJSON"
      case _ => throw new Error(s"Unsupported data type ${outputDataType.typeName}.")
    }
  }
}
