package com.databricks.mosaic.expressions.format

import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.io.{WKBReader, WKBWriter, WKTReader, WKTWriter}

object Conversions {
  def wkt2geom(input: Any): Geometry = {
    val wkt = input.asInstanceOf[UTF8String].toString
    val geom = new WKTReader().read(wkt)
    geom
  }

  def hex2geom(input: Any): Geometry = {
    val hex = input.asInstanceOf[UTF8String].toString
    val bytes = WKBReader.hexToBytes(hex)
    val geom = new WKBReader().read(bytes)
    geom
  }

  def geom2wkt(input: Geometry): UTF8String = {
    val wkt_payload = new WKTWriter().write(input)
    UTF8String.fromString(wkt_payload)
  }

  def geom2wkb(input: Geometry): Array[Byte] = {
    new WKBWriter().write(input)
  }

  def wkb2hex(input: Any): UTF8String = {
    val bytes = input.asInstanceOf[Array[Byte]]
    val hex_payload = WKBWriter.toHex(bytes)
    UTF8String.fromString(hex_payload)
  }

  def wkb2geom(input: Any): Geometry = {
    val bytes = input.asInstanceOf[Array[Byte]]
    val geom = new WKBReader().read(bytes)
    geom
  }
}
