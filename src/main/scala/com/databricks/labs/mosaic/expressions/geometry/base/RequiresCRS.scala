package com.databricks.labs.mosaic.expressions.geometry.base

import com.databricks.labs.mosaic.core.types._
import org.apache.spark.sql.types._

import scala.collection.immutable

trait RequiresCRS {

    val encodings: immutable.Seq[String] = List("COORDS", "GEOJSON")

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
            throw new Exception(
              s"""
                 |Input type encoding $inputTypeEncoding is not supported!
                 |Supported encodings are: ${encodings.mkString(", ")}""".stripMargin
            )
        }
    }

}
