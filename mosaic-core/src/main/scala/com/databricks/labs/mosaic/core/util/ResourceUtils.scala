package com.databricks.labs.mosaic.core.util

import java.io.BufferedInputStream
import scala.language.postfixOps

/**
 * Utility for reading resources from the classpath.
 * This is required for [[com.databricks.labs.mosaic.core.GenericServiceFactory[_]] to work.
 * All [[com.databricks.labs.mosaic.core.geometry.api.GeometryAPI]], [[com.databricks.labs.mosaic.core.index.IndexSystem]]
 * and [[com.databricks.labs.mosaic.core.raster.RasterAPI]] implementations are provided via META-INF/services.
 */
object ResourceUtils {

  def readResourceBytes(name: String): Array[Byte] = {
    val bis = new BufferedInputStream(getClass.getResourceAsStream(name))
    try Stream.continually(bis.read()).takeWhile(-1 !=).map(_.toByte).toArray
    finally bis.close()
  }

  def readResourceLines(name: String): Array[String] = {
    val bytes = readResourceBytes(name)
    val lines = new String(bytes).split("\n")
    lines
  }

}
