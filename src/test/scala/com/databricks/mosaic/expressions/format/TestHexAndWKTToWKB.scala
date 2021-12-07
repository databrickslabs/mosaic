package com.databricks.mosaic.expressions.format

import com.databricks.mosaic.expressions.mocks.{getHexRowsDf, getWKTRowsDf}
import com.databricks.mosaic.functions._
import com.databricks.mosaic.test.SparkTest
import org.apache.spark.sql.functions._
import org.scalatest.{FunSuite, Matchers}


class TestHexAndWKTToWKB extends FunSuite with SparkTest with Matchers {
  test("Conversion from Hex and WKT to WKB") {
    // To ensure we will be using same WKB representation
    // we convert hex to wkb and wkt to wkb and compare the outputs.

    val hexDf = getHexRowsDf
    val wktDf = getWKTRowsDf
    register(spark)

    val left = hexDf.withColumn("wkb", hex_to_wkb(col("hex"))).select("wkb").collect().map(_.toSeq.head)
    val right = wktDf.withColumn("wkb", wkt_to_wkb(col("wkt"))).select("wkb").collect().map(_.toSeq.head)

    left should contain theSameElementsAs right

    hexDf.createTempView("format_testing_left")
    wktDf.createTempView("format_testing_right")

    val left2 = spark.sql(
      "select hex_to_wkb(hex) as wkb from format_testing_left"
    ).collect().map(_.toSeq.head)
    val right2 = spark.sql(
      "select wkt_to_wkb(wkt) as wkb from format_testing_right"
    ).collect().map(_.toSeq.head)

    left2 should contain theSameElementsAs right2
  }
}
