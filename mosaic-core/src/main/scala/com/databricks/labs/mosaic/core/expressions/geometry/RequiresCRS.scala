package com.databricks.labs.mosaic.core.expressions.geometry

import com.databricks.labs.mosaic.core.MosaicException
import com.databricks.labs.mosaic.core.codegen.format.GeometryFormat
import org.apache.spark.sql.types._

import scala.collection.immutable

/**
 * Trait for checking if the input geometry is in a supported CRS.
 * Currently only supports GEOJSON.
 */
trait RequiresCRS {

  val encodings: immutable.Seq[String] = List("GEOJSON")

  def checkEncoding(dataType: DataType): Unit = {
    val inputTypeEncoding = GeometryFormat.getDefaultFormat(dataType)
    if (!encodings.contains(inputTypeEncoding)) {
      throw MosaicException.GeometryEncodingNotSupported(encodings, inputTypeEncoding)
    }
  }

}
