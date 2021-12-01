package com.databricks.mosaic.expressions.format

import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.io.{WKBReader, WKBWriter, WKTReader, WKTWriter}

/**
 * Provides methods that convert JTS geometries between different storage formats.
 * It allows easier testing for logic components of the expressions.
 * None of the methods use try/catch to avoid performance penalties at runtime.
 * Data type checking inside Catalyst will ensure required data types are respected.
 */
object Conversions {

  /**
   * Converts a WKT representation to a JTS [[Geometry]] instance.
   * @see [[Geometry]] provides more details on the Geometry API.
   * @param input is of [[Any]] type due to type erasure.
   *              The expected type is [[UTF8String]] and
   *              if that is not the case the call will fail.
   * @return An instance of [[Geometry]].
   */
  def wkt2geom(input: Any): Geometry = {
    val wkt = input.asInstanceOf[UTF8String].toString
    val geom = new WKTReader().read(wkt)
    geom
  }

  /**
   * Converts a WKB Hex representation to a JTS [[Geometry]] instance.
   * @see [[Geometry]] provides more details on the Geometry API.
   * @param input is of [[Any]] type due to type erasure.
   *              The expected type is [[UTF8String]] and
   *              if that is not the case the call will fail.
   * @return An instance of [[Geometry]].
   */
  def hex2geom(input: Any): Geometry = {
    val hex = input.asInstanceOf[UTF8String].toString
    val bytes = WKBReader.hexToBytes(hex)
    val geom = new WKBReader().read(bytes)
    geom
  }

  /**
   * Converts a JTS [[Geometry]] instance to a WKT representation in [[UTF8String]].
   * @see [[Geometry]] provides more details on the Geometry API.
   * @param input A [[Geometry]] instance to be converted.
   * @return An instance of [[UTF8String]] corresponding to a WKT encoding of said geometry.
   */
  def geom2wkt(input: Geometry): UTF8String = {
    val wkt_payload = new WKTWriter().write(input)
    UTF8String.fromString(wkt_payload)
  }

  /**
   * Converts a JTS [[Geometry]] instance to a WKB representation in [[Array]] of [[Byte]].
   * @see [[Geometry]] provides more details on the Geometry API.
   * @param input A [[Geometry]] instance to be converted.
   * @return An instance of [[Array]] of [[Byte]] corresponding to a WKB encoding of said geometry.
   */
  def geom2wkb(input: Geometry): Array[Byte] = {
    new WKBWriter().write(input)
  }

  /**
   * Converts a WKB representation in [[Array]] of [[Byte]] to a WKB Hex [[UTF8String]] instance.
   * @see [[Geometry]] provides more details on the Geometry API.
   * @param input An instance of [[Array]] of [[Byte]] to be converted.
   * @return An instance of [[UTF8String]] corresponding to a WKB Hex encoding of said geometry.
   */
  def wkb2hex(input: Any): UTF8String = {
    val bytes = input.asInstanceOf[Array[Byte]]
    val hex_payload = WKBWriter.toHex(bytes)
    UTF8String.fromString(hex_payload)
  }

  /**
   * Converts a WKB representation in [[Array]] of [[Byte]] to a JTS [[Geometry]] instance.
   * @see [[Geometry]] provides more details on the Geometry API.
   * @param input An instance of [[Array]] of [[Byte]] to be converted.
   * @return An instance of [[Geometry]].
   */
  def wkb2geom(input: Any): Geometry = {
    val bytes = input.asInstanceOf[Array[Byte]]
    val geom = new WKBReader().read(bytes)
    geom
  }
}
