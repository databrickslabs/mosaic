package com.databricks.mosaic.expresions.format

import com.databricks.mosaic.expresions.format.mocks.expressions.{getWKTRowsDf, hex_rows}
import com.databricks.mosaic.expressions.format.Conversions
import com.databricks.mosaic.functions._
import com.databricks.mosaic.test.SparkTest
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.{FunSuite, Matchers}


class TestWKBToHex extends FunSuite with SparkTest with Matchers {
  test("Conversion from WKB to WKB Hex") {
    // we ensure that we have WKB test data by converting WKT testing data to WKB
    val spark = SparkSession.builder().getOrCreate()
    val df = getWKTRowsDf.withColumn("wkb", wkt_to_wkb(col("wkt")))
    register(spark)

    val left = df.select(wkb_to_hex(col("wkb"))).collect()
    val leftGeoms = left.map(_.toSeq.head).map(s => Conversions.hex2geom(UTF8String.fromString(s.asInstanceOf[String])))
    val rightGeoms = hex_rows.map(_.head).map(s => Conversions.hex2geom(UTF8String.fromString(s)))

    leftGeoms should contain theSameElementsAs rightGeoms

    df.createTempView("format_testing")

    val left2 = spark.sql(
      "select wkb_to_hex(wkb) as hex from format_testing"
    ).collect()
    val leftGeoms2 = left2.map(_.toSeq.head).map(s => Conversions.hex2geom(UTF8String.fromString(s.asInstanceOf[String])))

    leftGeoms2 should contain theSameElementsAs rightGeoms
  }
}
