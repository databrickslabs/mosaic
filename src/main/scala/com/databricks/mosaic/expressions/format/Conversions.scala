package com.databricks.mosaic.expressions.format

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.io.geojson.{GeoJsonReader, GeoJsonWriter}
import org.locationtech.jts.io.{WKBReader, WKBWriter, WKTReader, WKTWriter}

/**
 * Provides methods that convert JTS geometries between different storage formats.
 * It allows easier testing for logic components of the expressions.
 * None of the methods use try/catch to avoid performance penalties at runtime.
 * Data type checking inside Catalyst will ensure required data types are respected.
 */
object Conversions {

  /**
   * Typed object is in charge of conversion between strongly typed instances.
   * This API is leveraged to deliver the untyped (Any => Any) API.
   */
  object typed {

    def hex2wkb(hex: String): Array[Byte] = {
      val bytes = WKBReader.hexToBytes(hex)
      bytes
    }

    /**
     * Converts [[String]] to a [[Geometry]] instance.
     * @param input WKT string to be parsed into a [[Geometry]].
     * @return An instance of [[Geometry]].
     */
    def wkt2geom(input: String): Geometry = {
      val geom = new WKTReader().read(input)
      geom
    }

    /**
     * Converts [[String]] containing hex encoding to a [[Geometry]] instance.
     * @param input Hex encoding to be parsed into a [[Geometry]].
     * @return An instance of [[Geometry]].
     */
    def hex2geom(input: String): Geometry = {
      val bytes = WKBReader.hexToBytes(input)
      val geom = new WKBReader().read(bytes)
      geom
    }

    /**
     * Converts [[Array]] of [[Byte]] containing wkb bytes to a [[Geometry]] instance.
     * @param input WKB encoding to be parsed into a [[Geometry]].
     * @return An instance of [[Geometry]].
     */
    def wkb2geom(input: Array[Byte]): Geometry = {
      val geom = new WKBReader().read(input)
      geom
    }

    /**
     * Converts GeoJSON encoded [[String]] to a [[Geometry]] instance.
     * @param input GeoJSON string to be parsed into a [[Geometry]].
     * @return An instance of [[Geometry]].
     */
    def geojson2geom(input: String): Geometry = {
      new GeoJsonReader().read(input)
    }
  }

  /**
   * Converts an instance of [[Geometry]] to the internal Hex encoding.
   * @see [[com.databricks.mosaic.core.types.HexType]] and [[AsHex]] for the hex encoding API.
   * @param input An instance of [[Geometry]] to be converted.
   * @return  An instance of [[InternalRow]] containing the hex encoding.
   */
  def geom2hex(input: Geometry): InternalRow = {
    val wkb = geom2wkb(input)
    val hexPayload = WKBWriter.toHex(wkb)
    InternalRow.fromSeq(Seq(UTF8String.fromString(hexPayload)))
  }

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
    typed.wkt2geom(wkt)
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
    val hexWrapper = input.asInstanceOf[InternalRow]
    val hex = hexWrapper.getString(0)
    typed.hex2geom(hex)
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
   * @return An instance of [[InternalRow]] containing a filed with a WKB Hex encoding of said geometry.
   */
  def wkb2hex(input: Any): InternalRow = {
    val bytes = input.asInstanceOf[Array[Byte]]
    val hexPayload = WKBWriter.toHex(bytes)
    InternalRow.fromSeq(Seq(UTF8String.fromString(hexPayload)))
  }

  /**
   * Converts a WKB representation in [[Array]] of [[Byte]] to a JTS [[Geometry]] instance.
   * @see [[Geometry]] provides more details on the Geometry API.
   * @param input An instance of [[Array]] of [[Byte]] to be converted.
   * @return An instance of [[Geometry]].
   */
  def wkb2geom(input: Any): Geometry = {
    val bytes = input.asInstanceOf[Array[Byte]]
    typed.wkb2geom(bytes)
  }

   /**
   * Converts a JTS [[Geometry]] instance to a GeoJSON [[UTF8String]].
   * @see [[Geometry]] provides more details on the Geometry API.
   * @param input A [[Geometry]] instance to be converted.
   * @return An [[UTF8String]] corresponding to a GeoJSON encoding of said geometry.
   */
  def geom2geojson(input: Geometry): InternalRow = {
    val geoJSONPayload = new GeoJsonWriter().write(input)
    InternalRow.fromSeq(Seq(UTF8String.fromString(geoJSONPayload)))
  }

  /**
   * Converts a GeoJSON representation to a JTS [[Geometry]] instance.
   * @see [[Geometry]] provides more details on the Geometry API.
   * @param input is of [[Any]] type due to type erasure.
   *              The expected type is [[UTF8String]] and
   *              if that is not the case the call will fail.
   * @return An instance of [[Geometry]].
   */
  def geojson2geom(input: Any): Geometry = {
    val jsonWrapper = input.asInstanceOf[InternalRow]
    val geoJSON = jsonWrapper.getString(0)
    typed.geojson2geom(geoJSON)
  }
}
