package com.databricks.mosaic.expressions.format

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.{FunSuite, Matchers}


class TestConversions() extends FunSuite with Matchers {
  val wkt = "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))"
  val hex = "00000000030000000100000005403E0000000000004024000000000000404400000000000040440000000000004034000000000000404400000000000040240000000000004034000000000000403E0000000000004024000000000000"

  test("Conversion from hex and wkt to Geom") {
    val wkt_geom = Conversions.wkt2geom(UTF8String.fromString(wkt))
    val hex_geom = Conversions.hex2geom(InternalRow.fromSeq(Seq(UTF8String.fromString(hex))))
    wkt_geom shouldEqual hex_geom
  }

  test("Conversion from hex and wkt to wkb") {
    val wkt_geom = Conversions.wkt2geom(UTF8String.fromString(wkt))
    val wkt_wkb = Conversions.geom2wkb(wkt_geom)
    val hex_geom = Conversions.hex2geom(InternalRow.fromSeq(Seq(UTF8String.fromString(hex))))
    val hex_wkb = Conversions.geom2wkb(hex_geom)
    //note that wkb encoding is system dependant
    //little endian vs big endian considerations are relevant for any WKB representation
    wkt_wkb shouldEqual hex_wkb
  }

  test("Conversion from hex to wkb to hex") {
    val hex_geom = Conversions.hex2geom(InternalRow.fromSeq(Seq(UTF8String.fromString(hex))))
    val hex_wkb = Conversions.geom2wkb(hex_geom)
    val new_hex = Conversions.wkb2hex(hex_wkb).getString(0)
    val wkb_geom = Conversions.wkb2geom(hex_wkb)
    wkb_geom shouldEqual hex_geom
    //note that wkb encoding is system dependant
    //little endian vs big endian considerations are relevant for any WKB representation
    new_hex shouldEqual hex
  }

  test("Conversion from wkt to wkb to wkt") {
    val wkt_geom = Conversions.wkt2geom(UTF8String.fromString(wkt))
    val wkt_wkb = Conversions.geom2wkb(wkt_geom)
    val new_wkt = Conversions.geom2wkt(wkt_geom).toString
    val wkb_geom = Conversions.wkb2geom(wkt_wkb)
    wkb_geom shouldEqual wkt_geom
    new_wkt shouldEqual wkt
  }
}
