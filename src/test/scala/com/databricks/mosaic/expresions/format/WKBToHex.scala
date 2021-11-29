package com.databricks.mosaic.expresions.format

import com.databricks.mosaic.expresions.format.mocks.expressions.{getWKTRowsDf, hex_rows}
import com.databricks.mosaic.expressions.format.Conversions
import com.databricks.mosaic.functions._
import com.databricks.mosaic.test.SparkTest
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.{FunSuite, Matchers}


class WKBToHex extends FunSuite with SparkTest with Matchers {
  test("Conversion from Hex to WKT") {
    val spark = SparkSession.builder().getOrCreate()
    val df = getWKTRowsDf(spark)
    register(spark)

    val left = df.select(
      wkb_to_hex(
        wkt_to_wkb(
          col("wkt")
        )
      )
    ).collect().map(_.toSeq.head).map(s => Conversions.hex2geom(UTF8String.fromString(s.asInstanceOf[String])))
    val right = hex_rows.map(_.head).map(s => Conversions.hex2geom(UTF8String.fromString(s)))

    left should contain theSameElementsAs right

    df.createTempView("format_testing")

    val left2 = spark.sql(
      "select wkb_to_hex(wkt_to_wkb(wkt)) as hex from format_testing"
    ).collect().map(_.toSeq.head).map(s => Conversions.hex2geom(UTF8String.fromString(s.asInstanceOf[String])))

    left2 should contain theSameElementsAs right
  }
}
